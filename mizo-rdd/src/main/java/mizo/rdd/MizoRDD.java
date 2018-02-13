package mizo.rdd;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mizo.core.IMizoRDDConfig;
import mizo.core.IMizoRelationParser;
import mizo.core.MizoJanusGraphRelationType;
import mizo.hbase.MizoRegionFamilyCellsIterator;
import mizo.hbase.MizoJanusGraphHBaseRelationParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by imrihecht on 12/10/16.
 */
public abstract class MizoRDD<TReturn> extends RDD<TReturn> implements Serializable {
    public static final Logger log = LoggerFactory.getLogger(MizoRDD.class);

    /**
     * Collection of region paths to be assigned to partitions by getPartitions
     */
    protected List<String> regionsPaths;

    /**
     * Mapping between relation-type-id to relation-type objects
     */
    protected Map<Long, MizoJanusGraphRelationType> relationTypes;

    /**
     * Config and tuning object for Mizo
     */
    protected IMizoRDDConfig config;

    public MizoRDD(SparkContext context, IMizoRDDConfig config, ClassTag<TReturn> classTag) {
        super(context, new ArrayBuffer<>(), classTag);

        if (!Strings.isNullOrEmpty(config.logConfigPath())) {
            PropertyConfigurator.configure(config.logConfigPath());
        }

        this.config = config;
        this.regionsPaths = getRegionsPaths(config.regionDirectoriesPath());
        this.relationTypes = loadRelationTypes(config.janusGraphConfigPath());
    }

    @Override
    public scala.collection.Iterator<TReturn> compute(Partition split, TaskContext context) {
        String regionEdgesFamilyPath = this.regionsPaths.get(split.index());
        log.info("Running Mizo on region #{} located at: {}", split.index(), regionEdgesFamilyPath);

        return createRegionIterator(createRegionRelationsIterator(regionEdgesFamilyPath));
    }

    /**
     * Creates a region iterator for a given region
     */
    protected Iterator<IMizoRelationParser> createRegionRelationsIterator(String regionEdgesFamilyPath) {
        try {
            MizoRegionFamilyCellsIterator cellsIterator = new MizoRegionFamilyCellsIterator(regionEdgesFamilyPath);

            return Iterators.transform(cellsIterator,
                    cell -> new MizoJanusGraphHBaseRelationParser(this.relationTypes, cell));

        } catch (IOException e) {
            log.error("Failed to initialized region relations reader due to inner exception: {}", e);

            return Collections.emptyIterator();
        }
    }

    /**
     * Create an iterator that returns the final output (edges, vertices etc)
     *
     * @param relationIterator Iterator that provides relations to parse
     * @return Concrete iterator that returns instances of edges, vertices etc
     */
    public abstract scala.collection.Iterator<TReturn> createRegionIterator(Iterator<IMizoRelationParser> relationIterator);


    @Override
    public Partition[] getPartitions() {
        return Iterators.toArray(IntStream
                .range(0, this.regionsPaths.size())
                .mapToObj(i -> (Partition) () -> i)
                .iterator(), Partition.class);
    }

    /**
     * Given a path with wildcards, where regions are located,
     * gets the paths of regions that satisfy these wildcards
     * @param regionDirectoryPaths Paths to get regions from, with wildcards
     * @return Collection of regions paths
     */
    protected static List<String> getRegionsPaths(String regionDirectoryPaths) {
        try {
            Path regionDirectory = new Path(regionDirectoryPaths);
            FileSystem fs = regionDirectory.getFileSystem(new Configuration());

            FileStatus[] fileStatuses = fs.globStatus(regionDirectory, new FSUtils.RegionDirFilter(fs));
            return Arrays.stream(fileStatuses)
                    .map(file -> file.getPath().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to get partitions due to inner exception: {}", e);

            return Collections.emptyList();
        }
    }

    /**
     * Given a path for Janus Graph config file, connects and gets the internal Janus Graph types,
     * converting them to MizoJanus GraphRelationTypes mapped by type-ids
     * @param janusGraphConfigPath Path to Janus Graph's config path
     * @return Mapping between relation type-ids to InternalRelationType instances
     */
    protected static HashMap<Long, MizoJanusGraphRelationType> loadRelationTypes(String janusGraphConfigPath) {
        JanusGraph g = JanusGraphFactory.open(janusGraphConfigPath);
        StandardJanusGraphTx tx = (StandardJanusGraphTx)g.buildTransaction().readOnly().start();

        HashMap<Long, MizoJanusGraphRelationType> relations = Maps.newHashMap();

        tx.query()
                .has(BaseKey.SchemaCategory, Contain.IN, Lists.newArrayList(JanusGraphSchemaCategory.values()))
                .vertices()
                .forEach(v -> {
                    if (v instanceof InternalRelationType)
                        relations.put(v.longId(), new MizoJanusGraphRelationType((InternalRelationType)v));
                });

        tx.close();

        try {
            ((StandardJanusGraph)g).getBackend().close();
        } catch (BackendException e) {
            e.printStackTrace();
        }

        return relations;
    }


}
