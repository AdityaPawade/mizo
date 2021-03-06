package mizo.hbase;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.util.stats.NumberUtil;
import mizo.core.IMizoRelationParser;
import mizo.core.MizoJanusGraphRelationType;
import mizo.hbase.patches.MizoReadArrayBuffer;
import org.apache.hadoop.hbase.Cell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by imrihecht on 12/9/16.
 */

public class MizoJanusGraphHBaseRelationParser implements IMizoRelationParser {
    private static final Logger log =
            LoggerFactory.getLogger(MizoJanusGraphHBaseRelationParser.class);
    private Iterator<LongObjectCursor<Object>> propertiesIterator;

    private Object nextPropertyValue;

    private Object propertyValue;

    /**
     * The type of this relation, in a Janus Graph object
     */
    private MizoJanusGraphRelationType relationType;

    /**
     * Id of the value we are parsing one of its relations
     */
    private final long vertexId;


    /**
     * Relation type ID
     */
    private long typeId;

    /**
     * Relation direction identifier
     */
    private IDHandler.DirectionID directionID;


    /**
     * Relation identifier - only if this relation is an edge
     */
    private long relationId;

    /**
     * ID of the other vertex - only if this relation is an edge
     */
    private long otherVertexId;

    /**
     * Janus Graph internal object for ID reading and writing
     */

    private final static IDManager ID_MANAGER = new IDManager(NumberUtil.getPowerOf2(32));

    /**
     * Mapping between relation type IDs and relation names
     * If a property - its name, if an edge - its label
     */
    private final Map<Long, MizoJanusGraphRelationType> relationTypes;

    private EdgeSerializer TITAN_EDGE_SERIALIZER = new EdgeSerializer(new StandardSerializer());


    private TypeInspector TITAN_TYPE_INSPECTOR = new TypeInspector() {
        @Override
        public RelationType getExistingRelationType(long id) {
            return relationTypes.get(id);
        }

        @Override
        public VertexLabel getExistingVertexLabel(long l) {
            return null;
        }

        @Override
        public boolean containsRelationType(String s) {
            return false;
        }

        @Override
        public RelationType getRelationType(String s) {
            return null;
        }
    };

    public MizoJanusGraphHBaseRelationParser(Map<Long, MizoJanusGraphRelationType> relationTypes, Cell cell) {
        this.relationTypes = relationTypes;

        this.vertexId = parseVertexId(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());

        Entry entry = createEntry(cell);
        extractRelationMetadata(entry.asReadBuffer());

        if (!isSystemType()) {
            RelationCache relationCache = TITAN_EDGE_SERIALIZER.parseRelation(entry, false, TITAN_TYPE_INSPECTOR);

            if (isProperty()) {
                propertyValue = relationCache.getValue();
            }
            else {
                relationId = relationCache.relationId;
                otherVertexId = relationCache.getOtherVertexId();
                propertiesIterator = relationCache.propertyIterator();
            }

            if (!isKnownType()) {
                log.warn("Unknown relation type (vertex-id={}, type-id={})", this.vertexId, this.typeId);
            }
        }
    }

    private Entry createEntry(Cell cell) {
        return StaticArrayEntry.of(new MizoReadArrayBuffer(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierOffset() + cell.getQualifierLength()),
                new MizoReadArrayBuffer(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength()));
    }

    /**
     * Extract data from the qualifier part of an HBase Cell
     */
    private void extractRelationMetadata(ReadBuffer buffer) {
        IDHandler.RelationTypeParse typeAndDirection = IDHandler.readRelationType(buffer);
        typeId = typeAndDirection.typeId;
        directionID = typeAndDirection.dirID;
        relationType = this.relationTypes.get(this.typeId);
    }

    /**
     * Parses the vertex id from the row array
     * This will be the same for cells that are under the same HBase row
     */
    private static long parseVertexId(byte[] rowArray, int rowOffset, int rowLength) {
        return ID_MANAGER.getKeyID(new StaticArrayBuffer(rowArray, rowOffset, rowOffset + rowLength));
    }

    @Override
    public Object readPropertyValue() {
        if (isProperty()) {
            return propertyValue;
        }

        return nextPropertyValue;
    }

    @Override
    public String readPropertyName() {
        if (isProperty()) {
            return relationType.name();
        }

        LongObjectCursor<Object> next = propertiesIterator.next();
        nextPropertyValue = next.value;
        return relationTypes.get(next.key).name();

    }

    @Override
    public boolean valueHasRemaining() {
        return propertiesIterator.hasNext();
    }


    /**
     * It is a type related to system?
     */
    @Override

    public boolean isSystemType() {
        return IDManager.isSystemRelationTypeId(typeId) ||
                typeId == BaseKey.VertexExists.longId() ||
                typeId == BaseLabel.VertexLabelEdge.longId() ||
                typeId == BaseKey.SchemaCategory.longId() ||
                typeId == BaseKey.SchemaDefinitionProperty.longId() ||
                typeId == BaseKey.SchemaDefinitionDesc.longId() ||
                typeId == BaseKey.SchemaName.longId() ||
                typeId == BaseLabel.SchemaDefinitionEdge.longId();
    }

    /**
     * Whether this relation type ID is not in the known relation-types mapping
     */
    @Override
    public boolean isKnownType() {
        return this.relationType != null;
    }

    @Override
    public long getTypeId() {
        return typeId;
    }

    @Override
    public String getTypeName() {
        return this.relationType.name();
    }

    @Override
    public long getRelationId() {
        return relationId;
    }

    @Override
    public long getVertexId() {
        return vertexId;
    }

    @Override
    public long getOtherVertexId() {
        return otherVertexId;
    }

    @Override
    public boolean isProperty() {
        return directionID.equals(IDHandler.DirectionID.PROPERTY_DIR);
    }

    @Override
    public boolean isOutEdge() {
        return directionID.equals(IDHandler.DirectionID.EDGE_OUT_DIR);
    }

    @Override
    public boolean isInEdge() {
        return directionID.equals(IDHandler.DirectionID.EDGE_IN_DIR);
    }
}

