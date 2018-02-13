package mizo.core;

import com.google.common.collect.Maps;
import org.janusgraph.graphdb.relations.RelationIdentifier;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by imriqwe on 26/08/2016.
 */
public class MizoEdge implements Serializable {
    private final String label;
    private final long relationId;
    private final MizoVertex vertex;
    private final boolean isOutEdge;
    private final long otherVertexId;
    private Map<String, Object> properties;
    private final long typeId;

    public MizoEdge(String label, long typeId, long relationId, boolean isOutEdge,
                    MizoVertex vertex, long otherVertexId) {

        this.label = label;
        this.typeId = typeId;
        this.relationId = relationId;
        this.isOutEdge = isOutEdge;
        this.vertex = vertex;
        this.otherVertexId = otherVertexId;
    }

    public String label() {
        return label;
    }

    public long relationId() {
        return relationId;
    }

    public MizoVertex vertex() {
        return vertex;
    }

    public boolean isOutEdge() {
        return isOutEdge;
    }

    public long getTypeId() {
        return typeId;
    }

    public long otherVertexId() {
        return otherVertexId;
    }

    public Map<String, Object> properties() {
        return properties;
    }

    public Map<String, Object> initializedProperties() {
        if (properties == null) {
            properties = Maps.newHashMap();
        }

        return properties;
    }

    /**
     * Creates a Janus Graph Edge Id of this edge
     */
    public String janusId() {
        return RelationIdentifier.get(new long[]{
                relationId,
                isOutEdge ? vertex.id() : otherVertexId,
                typeId,
                isOutEdge ? otherVertexId : vertex.id()}).toString();
    }
}
