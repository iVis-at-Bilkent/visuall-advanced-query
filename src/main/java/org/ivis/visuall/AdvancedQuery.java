package org.ivis.visuall;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class AdvancedQuery {
    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Context
    public Transaction tx;

    // The id property which is an internal unique identifier automatically assigned
    // to every node and relationship in the Neo4j database is deprecated and
    // related methods will be removed.
    public Relationship getRelationshipByElementId(String edgeElementId) {
        Relationship relationship = tx.getRelationshipByElementId(edgeElementId); // Assuming you're looking for a
        return relationship;
    }

    public Node getNodeByElementId(String nodeElementId) {
        Node node = tx.getNodeByElementId(nodeElementId);
        return node;
    }

    /**
     * finds the minimal sub-graph from given nodes
     *
     * @param elementIds   node element ids to find the minimal connected graph
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum length of a path between "ids"
     * @param isDirected   is direction important?
     * @param pageSize     return at maximum this number of nodes, always returns
     * @param currPage     which page do yoy want to return
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order elements by a property
     * @param orderDir     order direction
     * @return minimal sub-graph
     */

    public Iterable<RelationshipType> getAllRelationshipTypes() {
        Iterable<RelationshipType> relationshipTypes = tx.getAllRelationshipTypes();
        return relationshipTypes;
    }

    @Procedure(value = "graphOfInterest", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> graphOfInterest(@Name("elementIds") List<String> elementIds,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected,
            @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt,
            @Name("isIgnoreCase") boolean isIgnoreCase,
            @Name("orderBy") String orderBy, @Name("orderDir") long orderDir,
            @Name("timeMapping") Map<String, List<String>> timeMapping, @Name("startTime") long startTime,
            @Name("endTime") long endTime,
            @Name("inclusionType") long inclusionType, @Name("timeout") long timeout,
            @Name("idFilter") List<String> idFilter) throws Exception {
        long executionStarted = System.nanoTime();
        TimeChecker timeChecker = new TimeChecker(timeout);
        BFSOutput o1 = GoI(elementIds, ignoredTypes, lengthLimit, isDirected, timeChecker);
        this.endMeasuringTime("Graph of interest", executionStarted);
        o1.nodes.removeIf(elementIds::contains);
        executionStarted = System.nanoTime();
        o1 = this.filterByDate(o1, startTime, endTime, timeMapping, inclusionType);
        this.endMeasuringTime("Filter by date", executionStarted);
        executionStarted = System.nanoTime();
        int cntSrcNode = elementIds.size();
        int cntSkip = Math.max(0, (int) ((currPage - 1) * pageSize) - cntSrcNode);
        long numSrcNode2return = Math.min(pageSize, Math.max(0, cntSrcNode - (currPage - 1) * pageSize));
        Output o2;
        if (idFilter == null) {
            o2 = this.tableFiltering(o1, pageSize - numSrcNode2return, cntSkip, filterTxt, isIgnoreCase, orderBy,
                    orderDir);
            o2.totalNodeCount += cntSrcNode; // total node count should also include the source nodes
        } else {
            idFilter.addAll(elementIds);
            o2 = this.idFiltering(o1, idFilter);
        }

        this.endMeasuringTime("Filter by date", executionStarted);
        if (numSrcNode2return > 0) {
            int fromIdx = Math.max(0, (int) ((currPage - 1) * pageSize));
            int toIdx = Math.min(cntSrcNode, (int) (currPage * pageSize));
            this.addSourceNodes(o2, elementIds.subList(fromIdx, toIdx));
        }
        return Stream.of(o2);
    }

    /**
     * finds common nodes and edges on the downstream or upstream or undirected
     * stream
     *
     * @param elementIds   node element ids
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum depth
     * @param direction    INCOMING means upstream (regulator), OUTGOING means
     *                     downstream (target)
     * @param pageSize     return at maximum this number of nodes, always returns
     *                     "ids"
     * @param currPage     which page do yoy want to return
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order elements by a property
     * @param orderDir     order direction
     * @return sub-graph which contains sources and targets/regulators with paths
     *         between
     */
    @Procedure(value = "commonStream", mode = Mode.WRITE)
    @Description("From specified nodes forms founds common upstream/downstream (target/regulator)")
    public Stream<CommonStreamOutput> commonStream(@Name("elementIds") List<String> elementIds,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") long lengthLimit, @Name("direction") long direction,
            @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt,
            @Name("isIgnoreCase") boolean isIgnoreCase,
            @Name("orderBy") String orderBy, @Name("orderDir") long orderDir,
            @Name("timeMapping") Map<String, List<String>> timeMapping, @Name("startTime") long startTime,
            @Name("endTime") long endTime,
            @Name("inclusionType") long inclusionType, @Name("timeout") long timeout,
            @Name("idFilter") List<String> idFilter) throws Exception {
        long executionStarted = System.nanoTime();
        TimeChecker timeChecker = new TimeChecker(timeout);
        CSOutput o1 = this.CS(elementIds, ignoredTypes, lengthLimit, direction, timeChecker);
        this.endMeasuringTime("Common stream", executionStarted);
        o1.nodes.removeIf(elementIds::contains);
        BFSOutput bfsOutput = new BFSOutput(o1.nodes, o1.edges);
        executionStarted = System.nanoTime();
        bfsOutput = this.filterByDate(bfsOutput, startTime, endTime, timeMapping, inclusionType);
        this.endMeasuringTime("Filter by date", executionStarted);
        executionStarted = System.nanoTime();
        int cntSrcNode = elementIds.size();
        int cntSkip = Math.max(0, (int) ((currPage - 1) * pageSize) - cntSrcNode);
        long numSrcNode2return = Math.min(pageSize, Math.max(0, cntSrcNode - (currPage - 1) * pageSize));
        Output o2;
        if (idFilter == null) {
            o2 = this.tableFiltering(bfsOutput, pageSize - numSrcNode2return, cntSkip, filterTxt, isIgnoreCase, orderBy,
                    orderDir);
            o2.totalNodeCount += cntSrcNode; // total node count should also include the source nodes
        } else {
            idFilter.addAll(elementIds);
            o2 = this.idFiltering(bfsOutput, idFilter);
        }

        this.endMeasuringTime("Table filtering", executionStarted);
        if (numSrcNode2return > 0) {
            int fromIdx = Math.max(0, (int) ((currPage - 1) * pageSize));
            int toIdx = Math.min(cntSrcNode, (int) (currPage * pageSize));
            this.addSourceNodes(o2, elementIds.subList(fromIdx, toIdx));
        }
        return Stream.of(new CommonStreamOutput(o2, new ArrayList<>(o1.targetRegulatorNodes)));
    }

    /**
     * finds neighborhood from given nodes with length limit
     *
     * @param elementIds   node element ids to find the minimal connected graph
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum length of a path between "ids"
     * @param isDirected   is direction important?
     * @param pageSize     return at maximum this number of nodes, always returns
     *                     "ids"
     * @param currPage     which page do yoy want to return
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order elements by a property
     * @param orderDir     order direction
     * @return minimal sub-graph
     */
    @Procedure(value = "neighborhood", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> neighborhood(@Name("elementIds") List<String> elementIds,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected,
            @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt,
            @Name("isIgnoreCase") boolean isIgnoreCase,
            @Name("orderBy") String orderBy, @Name("orderDir") long orderDir,
            @Name("timeMapping") Map<String, List<String>> timeMapping, @Name("startTime") long startTime,
            @Name("endTime") long endTime,
            @Name("inclusionType") long inclusionType, @Name("timeout") long timeout,
            @Name("idFilter") List<String> idFilter) throws Exception {
        long executionStarted = System.nanoTime();
        TimeChecker timeChecker = new TimeChecker(timeout);
        BFSOutput o1 = neighborhoodBFS(elementIds, ignoredTypes, lengthLimit, isDirected, timeChecker);
        this.endMeasuringTime("neighborhood", executionStarted);
        o1.nodes.removeIf(elementIds::contains);
        executionStarted = System.nanoTime();
        o1 = this.filterByDate(o1, startTime, endTime, timeMapping, inclusionType);
        this.endMeasuringTime("Filter by date", executionStarted);
        executionStarted = System.nanoTime();
        int cntSrcNode = elementIds.size();
        int cntSkip = Math.max(0, (int) ((currPage - 1) * pageSize) - cntSrcNode);
        long numSrcNode2return = Math.min(pageSize, Math.max(0, cntSrcNode - (currPage - 1) * pageSize));
        Output o2;
        if (idFilter == null) {
            o2 = this.tableFiltering(o1, pageSize - numSrcNode2return, cntSkip, filterTxt, isIgnoreCase, orderBy,
                    orderDir);
            o2.totalNodeCount += cntSrcNode; // total node count should also include the source nodes
        } else {
            idFilter.addAll(elementIds);
            o2 = this.idFiltering(o1, idFilter);
        }
        this.endMeasuringTime("Filter by date", executionStarted);
        if (numSrcNode2return > 0) {
            int fromIdx = Math.max(0, (int) ((currPage - 1) * pageSize));
            int toIdx = Math.min(cntSrcNode, (int) (currPage * pageSize));
            this.addSourceNodes(o2, elementIds.subList(fromIdx, toIdx));
        }
        return Stream.of(o2);
    }

    /**
     * finds all the paths containing a prefix of a specified segments in pangenome
     * graph
     * 
     * @param sequenceChain             ordered chain of sequences that we want to
     *                                  match within
     *                                  the graph
     * @param maxJumpLength             maximum allowed jump length between
     *                                  non-matching
     *                                  segments in the path
     * @param minSubsequenceMatchLength minimum length of the match of the
     *                                  subsequence path length
     * @param ignoredTypes              list of strings which are ignored types
     * @param pageSize                  return at maximum this number of nodes,
     *                                  always returns
     *                                  "ids"
     * @param currPage                  which page do yoy want to return
     * @param timeout                   maximum time to execute the procedure
     * @return all maximal paths in the pangenome graph
     */
    @Procedure(value = "sequenceChainSearch", mode = Mode.WRITE)
    @Description("finds all the paths containing a prefix of a specified segments in pangenome graph")
    public Stream<Output> sequenceChainSearch(@Name("sequenceChain") List<String> sequenceChain,
            @Name("maxJumpLength") long maxJumpLength,
            @Name("minSubsequenceMatchLength") long minSubsequenceMatchLength,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("timeout") long timeout)
            throws Exception {
        long executionStarted = System.nanoTime();
        TimeChecker timeChecker = new TimeChecker(timeout);
        BFSOutput o1 = findSequenceChain(sequenceChain, (int) maxJumpLength,
                (int) minSubsequenceMatchLength, ignoredTypes, timeChecker);
        this.endMeasuringTime("Sequence chain search", executionStarted);
        // Convert the BFSOutput to Output
        Output o2 = new Output();
        o2.nodes = new ArrayList<>();
        for (String nodeId : o1.nodes) {
            o2.nodes.add(getNodeByElementId(nodeId));
        }
        o2.edges = new ArrayList<>();
        for (String edgeId : o1.edges) {
            o2.edges.add(getRelationshipByElementId(edgeId));
        }
        o2.totalNodeCount = o2.nodes.size();
        return Stream.of(o2);
    }

    /**
     * testing for error
     */
    @Procedure(value = "error", mode = Mode.WRITE)
    @Description("testing for error")
    public Stream<Output> error() {
        throw new RuntimeException("Error testing 123");
    }

    /**
     * Since result could be big, gives results page by page
     *
     * @param o            to be paginated
     * @param pageSize     size of a page
     * @param skip         number of elements to skip
     * @param filterTxt    filtering text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order by a property
     * @param orderDir     order direction
     * @return a page of o
     */
    private Output tableFiltering(BFSOutput o, long pageSize, int skip, String filterTxt, boolean isIgnoreCase,
            String orderBy, long orderDir) {
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);
        r.totalNodeCount = r.nodes.size();
        // sort and paginate
        OrderDirection dir = num2OrderDir(orderDir);
        if (orderBy != null && dir != OrderDirection.NONE) {
            r.nodes.sort((n1, n2) -> {
                if (!n1.hasProperty(orderBy) || !n2.hasProperty(orderBy)) {
                    return 0;
                }
                Object o1 = n1.getProperty(orderBy);
                Object o2 = n2.getProperty(orderBy);

                if (o1.getClass() == Integer.class) {
                    return ((Integer) o1).compareTo((Integer) o2);
                } else if (o1.getClass() == String.class) {
                    return ((String) o1).compareTo((String) o2);
                } else if (o1.getClass() == Double.class) {
                    return ((Double) o1).compareTo((Double) o2);
                } else if (o1.getClass() == Float.class) {
                    return ((Float) o1).compareTo((Float) o2);
                }
                return 0;
            });
            if (dir == OrderDirection.DESC) {
                Collections.reverse(r.nodes);
            }
        }
        int nodeCount = r.nodes.size();
        int fromIdx = Math.min(Math.max(0, skip), nodeCount);
        int toIdx = Math.min(fromIdx + (int) pageSize, nodeCount);

        r.nodes = r.nodes.subList(fromIdx, toIdx);

        // get all edges
        for (String edgeElementId : o.edges) {
            Relationship e = getRelationshipByElementId(edgeElementId);

            r.edges.add(e);
            r.edgeClass.add(e.getType().name());
            r.edgeElementId.add(edgeElementId);
            String src = e.getStartNode().getElementId();
            String tgt = e.getEndNode().getElementId();
            ArrayList<String> l = new ArrayList<>();
            l.add(src);
            l.add(tgt);
            r.edgeSourceTargets.add(l);
        }

        // set meta properties for nodes
        for (Node n : r.nodes) {
            r.nodeClass.add(n.getLabels().iterator().next().name());
            r.nodeElementId.add(n.getElementId());
        }

        return r;
    }

    /**
     * Since result could be big, gives results page by page
     *
     * @param o        to be paginated
     * @param idFilter list of node elementIds
     * @return only the edges and nodes related with the the `idFilter`
     */
    private Output idFiltering(BFSOutput o, List<String> idFilter) {
        HashSet<String> elementIds = new HashSet<>(idFilter);
        HashSet<String> edges2 = new HashSet<>(o.edges);

        for (String edgeElementId : edges2) {
            Relationship e = getRelationshipByElementId(edgeElementId);
            String src = e.getStartNode().getElementId();
            String tgt = e.getEndNode().getElementId();
            if (!elementIds.contains(src) && !elementIds.contains(tgt)) {
                o.edges.remove(edgeElementId);
            }
        }

        HashSet<String> nodes2 = new HashSet<>(o.nodes);
        for (String nodeElementId : nodes2) {
            if (!elementIds.contains(nodeElementId)) {
                o.nodes.remove(nodeElementId);
            }
        }

        Output r = new Output();

        for (String nodeId : o.nodes) {
            Node n = getNodeByElementId(nodeId);
            r.nodes.add(n);
            r.nodeElementId.add(nodeId);
            r.nodeClass.add(n.getLabels().iterator().next().name());
        }

        for (String edgeElementId : o.edges) {
            Relationship e = getRelationshipByElementId(edgeElementId);
            r.edges.add(e);
            r.edgeClass.add(e.getType().name());
            r.edgeElementId.add(edgeElementId);
            String src = e.getStartNode().getElementId();
            String tgt = e.getEndNode().getElementId();
            ArrayList<String> l = new ArrayList<>();
            l.add(src);
            l.add(tgt);
            r.edgeSourceTargets.add(l);
        }
        r.totalNodeCount = r.nodes.size();

        return r;
    }

    /**
     * filter by text using simple text matching
     *
     * @param o            to be filtered
     * @param filterTxt    text for filtering
     * @param isIgnoreCase should ignore case?
     * @return filtered "o"
     */
    private Output filterByTxt(BFSOutput o, String filterTxt, boolean isIgnoreCase) {
        Output r = new Output();
        // filter by text
        if (filterTxt != null && filterTxt.length() > 0) {
            if (isIgnoreCase) {
                filterTxt = filterTxt.toLowerCase();
            }
            for (String id : o.nodes) {
                Node n = getNodeByElementId(id);
                List<String> l = new ArrayList<>();
                for (Object p : n.getAllProperties().values()) {
                    if (p.getClass().isArray()) {
                        StringBuilder s = new StringBuilder();
                        for (Object o2 : ((Object[]) p)) {
                            s.append(o2.toString());
                        }
                        l.add(s.toString());
                    } else {
                        l.add(p.toString());
                    }
                }

                boolean isPassed = false;
                for (String s : l) {
                    if (isIgnoreCase) {
                        if (s.toLowerCase().contains(filterTxt)) {
                            isPassed = true;
                            break;
                        }
                    } else {
                        if (s.contains(filterTxt)) {
                            isPassed = true;
                            break;
                        }
                    }
                }
                if (isPassed) {
                    r.nodes.add(n);
                }
            }
        } else {
            for (String id : o.nodes) {
                r.nodes.add(getNodeByElementId(id));
            }
        }
        return r;
    }

    /**
     * filter by a datetime range
     *
     * @param o           BFS output
     * @param d1          Unix time formatted time for start date
     * @param d2          Unix time formatted time for end date
     * @param timeMapping mapping to remark start and end date properties for an
     *                    entity (Node or Relationship)
     * @return filtered BFS output
     */
    private BFSOutput filterByDate(BFSOutput o, long d1, long d2, Map<String, List<String>> timeMapping,
            long inclusionType) {
        if (d1 == d2) {
            return o;
        }
        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        for (String id : o.nodes) {
            Node n = getNodeByElementId(id);
            String nodeType = n.getLabels().iterator().next().name();
            List<String> l = timeMapping.get(nodeType);
            if (l == null) {
                r.nodes.add(id);
            } else {
                this.addIfInRange(id, d1, d2, inclusionType, n, l, r.nodes);
            }
        }

        for (String elementId : o.edges) {
            Relationship e = getRelationshipByElementId(elementId);
            String edgeType = e.getType().name();
            List<String> l = timeMapping.get(edgeType);
            if (l == null) {
                r.edges.add(elementId);
            } else {
                this.addIfInRange(elementId, d1, d2, inclusionType, e, l, r.edges);
            }
        }
        return r;
    }

    /**
     * finds all the paths containing a prefix of a specified segments in pangenome
     * graph
     * 
     * @param sequenceChain ordered chain of sequences that we want to match within
     *                      the graph
     * @param maxJumpLength maximum allowed jump length between non-matching
     *                      segments in the path
     * @param ignoredTypes  list of strings which are ignored types
     * @param isDirected    is directed?
     * @param timeChecker   time checker
     * @return all maximal paths in the pangenome graph as BFSOutput
     */
    private BFSOutput findSequenceChain(List<String> sequenceChain, int maxJumpLength,
            int minSubsequenceMatchLength, List<String> ignoredTypes, TimeChecker timeChecker) throws Exception {
        // First find the segment nodes that contain the first sequence by searching the
        // whole graph
        HashSet<String> seedSequenceSegments = new HashSet<>();

        try (Transaction tx = db.beginTx()) {
            for (Node n : tx.getAllNodes()) {
                if (n.hasProperty("segmentData") &&
                        n.getProperty("segmentData").toString().contains(
                                sequenceChain.get(0))) {
                    seedSequenceSegments.add(n.getElementId());
                }
            }
        }

        // Initialize the resulting path set empty
        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());

        // For each seed sequence segment, find the paths that contain the rest of
        // the sequences in the sequence chain
        for (String seedSequenceSegment : seedSequenceSegments) {
            // Merge the resulting paths with the paths found for the current seed sequence
            // segment
            BFSOutput o = findSequenceChainFromSeed(seedSequenceSegment, sequenceChain,
                    maxJumpLength, minSubsequenceMatchLength, ignoredTypes, timeChecker);

            // Merge the resulting paths with the paths found for the current seed sequence
            // o U r
            r.nodes.addAll(o.nodes);
            r.edges.addAll(o.edges);
        }

        return r; // return the resulting path set
    }

    /**
     * Finds the paths that contain the rest of the sequences in the sequence chain
     * starting from the given seed sequence segment.
     * 
     * @param seedSequenceSegment       the seed sequence segment to start the
     *                                  search from
     * @param sequenceChain             the sequence chain to search for
     * @param maxJumpLength             maximum allowed jump length between
     *                                  non-matching
     *                                  segments in the path
     * @param ignoredTypes              list of strings which are ignored types
     * @param minSubsequenceMatchLength minimum length of the match of the
     *                                  subsequence
     *                                  path length
     * @param timeChecker               time checker
     * 
     * @return all maximal paths in the pangenome graph as BFSOutput given the seed
     *         sequence segment
     */
    private BFSOutput findSequenceChainFromSeed(String seedSequenceSegment, List<String> sequenceChain,
            int maxJumpLength, int minSubsequenceMatchLength, List<String> ignoredTypes, TimeChecker timeChecker)
            throws Exception {
        // Initialize explored set to keep track of visited nodes
        HashSet<String> explored = new HashSet<>();

        // Create a local class for priority queue elements
        class PQElement implements Comparable<PQElement> {
            String nodeElementId;
            int sequenceChainIndex; // used as the priority of items (lower values are higher priority) in the
                                    // queue, keeps the index of the last specified sequence that we matched so far
                                    // during our traversal
            int segmentDataSequenceIndex;
            int currentJumpLength; // gap count where nodes on the path do not contain any of the sequence from the
                                   // chain currently being searched
            List<String> path; // list of node element ids that form the path
            List<String> edges; // list of edge element ids that form the path

            public PQElement(String nodeElementId,
                    int sequenceChainIndex,
                    int segmentDataSequenceIndex,
                    int currentJumpLength, List<String> path, List<String> edges) {
                this.nodeElementId = nodeElementId;
                this.sequenceChainIndex = sequenceChainIndex;
                this.segmentDataSequenceIndex = segmentDataSequenceIndex;
                this.currentJumpLength = currentJumpLength;
                this.path = path;
                this.edges = edges;
            }

            @Override
            public int compareTo(PQElement o) {
                // Compare the priority of two elements in the priority queue based on the
                // sequence chain index (lower values are higher priority)
                return Integer.compare(this.sequenceChainIndex, o.sequenceChainIndex);
            }
        }

        // Initialize empty priority queue for BFS
        PriorityQueue<PQElement> pq = new PriorityQueue<>();
        // Initialize the resulting path set empty
        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>()); // nodes and edges
        // Set the starting seedSequenceSegment visited
        explored.add(seedSequenceSegment);
        // Initialize the priority queue with the seed sequence segment with an empty
        // path(empty list of node element ids)
        pq.add(new PQElement(seedSequenceSegment, 0, 0, 0, new ArrayList<>(), new ArrayList<>()));

        // While the priority queue is not empty
        while (!pq.isEmpty()) {
            // Get the element with the highest priority
            PQElement curr = pq.poll();

            // Check whether the current node contains the sequence from the sequence chain
            // that we are currently searching for
            Node n = getNodeByElementId(curr.nodeElementId);
            String segmentData = n.getProperty("segmentData").toString();
            int sequenceStartIndex = -1;
            String sequence = "";

            // Check whether the node contains the sequence starting from the given
            // segmentDataSequenceIndex in the segmentData as a substring
            if (curr.sequenceChainIndex < sequenceChain.size()) {
                sequence = sequenceChain.get((int) curr.sequenceChainIndex);
                sequenceStartIndex = segmentData.indexOf(sequence, (int) curr.segmentDataSequenceIndex);
            }

            // If the sequence is found in the segmentData
            if (sequenceStartIndex >= 0) {
                // Check whether the current node is the last node in the path to avoid adding
                // the same node to the path multiple times
                boolean isLastNodeOfPathCurrentNode = false;
                if (curr.path.size() > 0) {
                    isLastNodeOfPathCurrentNode = curr.nodeElementId.equals(curr.path.get(curr.path.size() - 1));
                }

                // If the current node is not the last node in the path
                if (!isLastNodeOfPathCurrentNode) {
                    // Remove the nodes in the path from the resulting path set if the matched
                    // subsequences are higher than the minimum subsequence match length
                    if (curr.sequenceChainIndex + 1 >= minSubsequenceMatchLength) {
                        r.nodes.removeAll(curr.path);
                        r.edges.removeAll(curr.edges);
                    }

                    // Add the current node to the path
                    curr.path.add(curr.nodeElementId);

                    // Add the nodes and edges in the path to the resulting path set if the matched
                    // subsequences are higher than the minimum subsequence match length
                    if (curr.sequenceChainIndex + 1 >= minSubsequenceMatchLength) {
                        r.nodes.addAll(curr.path);
                        r.edges.addAll(curr.edges);
                    }
                }

                // add current node with next sequence index and segment data index to the queue
                pq.add(new PQElement(curr.nodeElementId, curr.sequenceChainIndex + 1,
                        sequenceStartIndex + sequence.length(),
                        curr.currentJumpLength, new ArrayList<>(curr.path), new ArrayList<>(curr.edges)));
            } else {
                // Check whether the current node is the last node in the path to avoid adding
                // the same node to the path multiple times
                boolean isLastNodeOfPathCurrentNode = false;
                if (curr.path.size() > 0) {
                    isLastNodeOfPathCurrentNode = curr.nodeElementId.equals(curr.path.get(curr.path.size() - 1));
                }

                // Add the current node to the path
                if (!isLastNodeOfPathCurrentNode) {
                    curr.currentJumpLength++;
                    // Check whether the jump length is less than the maximum allowed jump length
                    if (curr.currentJumpLength > maxJumpLength) {
                        continue;
                    }
                    curr.path.add(curr.nodeElementId);
                }

                // Loop through the outgoing neighbors of the current node
                for (Relationship e : n.getRelationships(Direction.OUTGOING)) {
                    Node neighbor = e.getEndNode();
                    String neighborElementId = neighbor.getElementId();

                    // Check whether the neighbor node is not visited
                    if (!explored.contains(neighborElementId)) {
                        // Add the neighbor node to the visited set
                        explored.add(neighborElementId);

                        // Add the edge to the path
                        List<String> newEdges = new ArrayList<>(curr.edges);
                        newEdges.add(e.getElementId());

                        // Add the neighbor node with the current sequence chain index and segment data
                        // index to the queue
                        pq.add(new PQElement(neighborElementId, curr.sequenceChainIndex,
                                0, curr.currentJumpLength,
                                new ArrayList<>(curr.path), newEdges));
                    }
                }
            }
        }

        return r; // return the resulting path set
    }

    private void addIfInRange(String elementId, long d1, long d2, long inclusionType, Entity e, List<String> propNames,
            HashSet<String> set) {
        String propStartName = propNames.get(0);
        String propEndName = propNames.get(1);
        boolean has1 = e.hasProperty(propStartName);
        boolean has2 = e.hasProperty(propEndName);
        long start = Long.MIN_VALUE;
        long end = Long.MAX_VALUE;
        if (has1) {
            Object o = e.getProperty(propStartName);
            if (o instanceof Long) {
                start = (long) o;
            } else if (o instanceof Double) {
                start = ((Double) o).longValue();
            }
        }
        if (has2) {
            Object o = e.getProperty(propEndName);
            if (o instanceof Long) {
                end = (long) o;
            } else if (o instanceof Double) {
                end = ((Double) o).longValue();
            }
        }

        if (inclusionType == 0 && start <= d2 && end >= d1) { // the range and object life-time are overlapping
            set.add(elementId);
        } else if (inclusionType == 1 && d1 <= start && d2 >= end) { // the range contains the object life-time
            set.add(elementId);
        } else if (inclusionType == 2 && start <= d1 && end >= d2) { // the range contained by the object life-time
            set.add(elementId);
        }
    }

    private void addSourceNodes(Output o, List<String> elementIds) {
        // insert source nodes
        for (String elementId : elementIds) {
            Node n = getNodeByElementId(elementId);
            o.nodeElementId.add(elementId);
            o.nodes.add(n);
            o.nodeClass.add(n.getLabels().iterator().next().name());
        }
    }

    /**
     * finds neighborhood from given node elementIds with the length limit
     *
     * @param elementIds   source nodes to find the minimal sub-graph
     * @param ignoredTypes list of strings which are ignored types
     * @param lengthLimit  maximum left of a path between any 2 source nodes
     * @param isDirected   is directed?
     * @return a set of nodes and edges which is sub-graph
     */
    private BFSOutput neighborhoodBFS(List<String> elementIds, List<String> ignoredTypes, long lengthLimit,
            boolean isDirected,
            TimeChecker timeChecker) throws Exception {
        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        HashSet<String> srcNodes = new HashSet<>(elementIds);
        HashSet<String> visitedEdges = new HashSet<>();
        Queue<String> queue = new LinkedList<>(elementIds);

        RelationshipType[] allowedEdgeTypesArr = getValidRelationshipTypes(ignoredTypes);
        HashSet<String> ignoredTypesSet = new HashSet<>(ignoredTypes);
        Direction dir = Direction.OUTGOING;
        if (!isDirected) {
            dir = Direction.BOTH;
        }
        int currDepth = 0;
        int queueSizeBeforeMe = 0;
        boolean isPendingDepthIncrease = false;
        while (!queue.isEmpty()) {
            if (queueSizeBeforeMe == 0) {
                currDepth++;
                isPendingDepthIncrease = true;
            }
            if (currDepth == lengthLimit + 1) {
                break;
            }
            Node curr = getNodeByElementId(queue.remove());
            queueSizeBeforeMe--;

            Iterable<Relationship> edges = curr.getRelationships(dir, allowedEdgeTypesArr);
            timeChecker.checkTime();
            for (Relationship e : edges) {
                Node n = e.getOtherNode(curr);
                String elementId = n.getElementId();
                String edgeElementId = e.getElementId();
                boolean isIgnore = !srcNodes.contains(elementId) && this.isNodeIgnored(n, ignoredTypesSet);
                if (isIgnore || visitedEdges.contains(edgeElementId)) {
                    continue;
                }
                r.nodes.add(elementId);
                r.edges.add(e.getElementId());
                visitedEdges.add(edgeElementId);
                if (isPendingDepthIncrease) {
                    queueSizeBeforeMe = queue.size();
                    isPendingDepthIncrease = false;
                }
                queue.add(elementId);
                timeChecker.checkTime();
            }
        }
        return r;
    }

    /**
     * finds Graph of Interest from given node ids
     *
     * @param elementIds   source nodes to find the minimal sub-graph
     * @param ignoredTypes list of strings which are ignored types
     * @param lengthLimit  maximum left of a path between any 2 source nodes
     * @param isDirected   is directed?
     * @return a set of nodes and edges which is sub-graph
     */
    private BFSOutput GoI(List<String> elementIds, List<String> ignoredTypes, long lengthLimit, boolean isDirected,
            TimeChecker timeChecker) throws Exception {
        HashSet<String> elementIdSet = new HashSet<>(elementIds);
        HashMap<String, LabelData> edgeLabels = new HashMap<>();
        HashMap<String, LabelData> nodeLabels = new HashMap<>();
        for (String elementId : elementIds) {
            nodeLabels.put(elementId, new LabelData(0, 0));
        }

        BFSOutput o1 = this.GoI_BFS(nodeLabels, edgeLabels, elementIdSet, ignoredTypes, lengthLimit, Direction.OUTGOING,
                isDirected, false, timeChecker, elementIdSet);
        BFSOutput o2 = this.GoI_BFS(nodeLabels, edgeLabels, elementIdSet, ignoredTypes, lengthLimit, Direction.INCOMING,
                isDirected, false, timeChecker, elementIdSet);
        o1.edges.addAll(o2.edges);
        o1.nodes.addAll(o2.nodes);

        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        for (String edgeElementId : o1.edges) {
            if (edgeLabels.get(edgeElementId).fwd + edgeLabels.get(edgeElementId).rev <= lengthLimit) {
                r.edges.add(edgeElementId);
            }
        }
        for (String nodeElementId : o1.nodes) {
            if (nodeLabels.get(nodeElementId).fwd + nodeLabels.get(nodeElementId).rev <= lengthLimit) {
                r.nodes.add(nodeElementId);
            }
        }
        r.nodes.addAll(elementIds);
        r = this.removeOrphanEdges(r);
        this.purify(elementIdSet, r);
        r = this.removeOrphanEdges(r);

        return r;
    }

    /**
     * find common stream (up/down/undirected)
     *
     * @param elementIds   database element ids of nodes
     * @param ignoredTypes list of strings which are ignored types
     * @param lengthLimit  maximum depth
     * @param direction    should be 0 or 1
     * @return Outputs a list of nodes and edges (Cypher does not accept HashSet)
     */
    private CSOutput CS(List<String> elementIds, List<String> ignoredTypes, long lengthLimit, long direction,
            TimeChecker timeChecker) throws Exception {

        HashSet<String> resultNodes = new HashSet<>();

        HashSet<String> candidates = new HashSet<>();
        HashMap<String, Integer> node2Reached = new HashMap<>();
        Direction d = this.num2Dir(direction);
        for (String elementId : elementIds) {
            candidates.addAll(this.CS_BFS(node2Reached, elementId, ignoredTypes, lengthLimit, d));
        }
        int size = elementIds.size();
        for (String elementId : candidates) {
            Integer i = node2Reached.get(elementId);
            if (i != null && i == size) {
                resultNodes.add(elementId);
            }
        }

        HashMap<String, LabelData> edgeLabels = new HashMap<>();
        HashMap<String, LabelData> nodeLabels = new HashMap<>();
        HashSet<String> s1 = new HashSet<>(elementIds);

        for (String elementId : s1) {
            nodeLabels.put(elementId, new LabelData(0, lengthLimit + 1));
        }
        for (String elementId : resultNodes) {
            nodeLabels.put(elementId, new LabelData(lengthLimit + 1, 0));
        }

        HashSet<String> unavoidable = new HashSet<>(resultNodes);
        unavoidable.addAll(s1);

        BFSOutput o1, o2;
        if (d == Direction.OUTGOING) { // means common target
            if (s1.size() < resultNodes.size()) {
                o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.OUTGOING, true, false,
                        timeChecker, unavoidable);
                o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.INCOMING, true,
                        true, timeChecker, unavoidable);
            } else {
                o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.INCOMING, true,
                        false, timeChecker, unavoidable);
                o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.OUTGOING, true, true,
                        timeChecker, unavoidable);
            }
        } else if (d == Direction.INCOMING) { // means common regulator
            for (String elementId : s1) {
                nodeLabels.put(elementId, new LabelData(lengthLimit + 1, 0));
            }
            for (String elementId : resultNodes) {
                nodeLabels.put(elementId, new LabelData(0, lengthLimit + 1));
            }
            if (s1.size() < resultNodes.size()) {
                o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.INCOMING, true, false,
                        timeChecker, unavoidable);
                o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.OUTGOING, true,
                        true, timeChecker, unavoidable);
            } else {
                o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.OUTGOING, true,
                        false, timeChecker, unavoidable);
                o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.INCOMING, true, true,
                        timeChecker, unavoidable);
            }
        } else {
            if (s1.size() < resultNodes.size()) {
                o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.OUTGOING, false, false,
                        timeChecker, unavoidable);
                o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.INCOMING, false,
                        true, timeChecker, unavoidable);
            } else {
                o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.INCOMING, false,
                        false, timeChecker, unavoidable);
                o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.OUTGOING, false, true,
                        timeChecker, unavoidable);
            }
        }

        // merge result sets
        o1.edges.addAll(o2.edges);
        o1.nodes.addAll(o2.nodes);

        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        for (String edgeElementId : o1.edges) {
            if (edgeLabels.get(edgeElementId).fwd + edgeLabels.get(edgeElementId).rev <= lengthLimit) {
                r.edges.add(edgeElementId);
            }
        }

        for (String nodeElementId : o1.nodes) {
            if (nodeLabels.get(nodeElementId).fwd + nodeLabels.get(nodeElementId).rev <= lengthLimit) {
                r.nodes.add(nodeElementId);
            }
        }
        r.nodes.addAll(elementIds);
        r.nodes.addAll(resultNodes);
        r = this.removeOrphanEdges(r);
        s1.addAll(resultNodes);
        this.purify(s1, r);
        r = this.removeOrphanEdges(r);

        return new CSOutput(r.nodes, r.edges, resultNodes);
    }

    /**
     * @param node2Reached keeps reached data for each node
     * @param nodeId       id of node to make BFS
     * @param ignoredTypes list of strings which are ignored types
     * @param depthLimit   deepness of BFS
     * @param dir          direction of BFS
     * @return HashSet<Long>
     */
    private HashSet<String> CS_BFS(HashMap<String, Integer> node2Reached, String nodeElementId,
            List<String> ignoredTypes,
            long depthLimit, Direction dir) {
        HashSet<String> visitedNodes = new HashSet<>();
        visitedNodes.add(nodeElementId);

        Queue<String> queue = new LinkedList<>();
        queue.add(nodeElementId);

        RelationshipType[] allowedEdgeTypesArr = getValidRelationshipTypes(ignoredTypes);
        HashSet<String> ignoredTypesSet = new HashSet<>(ignoredTypes);

        int currDepth = 0;
        int queueSizeBeforeMe = 0;
        boolean isPendingDepthIncrease = false;
        while (!queue.isEmpty()) {

            if (queueSizeBeforeMe == 0) {
                currDepth++;
                isPendingDepthIncrease = true;
            }
            if (currDepth == depthLimit + 1) {
                break;
            }

            Node curr = getNodeByElementId(queue.remove());
            queueSizeBeforeMe--;

            Iterable<Relationship> edges = curr.getRelationships(dir, allowedEdgeTypesArr);
            for (Relationship e : edges) {
                Node n = e.getOtherNode(curr);
                String elementId = n.getElementId();

                if ((elementId != nodeElementId && this.isNodeIgnored(n, ignoredTypesSet))
                        || visitedNodes.contains(elementId)) {
                    continue;
                }

                Integer cnt = node2Reached.get(elementId);
                if (cnt == null) {
                    cnt = 0;
                }
                node2Reached.put(elementId, cnt + 1);
                visitedNodes.add(elementId);
                if (isPendingDepthIncrease) {
                    queueSizeBeforeMe = queue.size();
                    isPendingDepthIncrease = false;
                }

                queue.add(elementId);
            }
        }
        return visitedNodes;
    }

    /**
     * map number to direction, 0: OUTGOING (downstream), 1: INCOMING (upstream), 2:
     * BOTH
     *
     * @param n number for enum
     * @return Direction
     */
    private Direction num2Dir(long n) {

        Direction d = Direction.BOTH;
        if (n == 0) {
            d = Direction.OUTGOING; // means downstream
        } else if (n == 1) {
            d = Direction.INCOMING; // means upstream
        }
        return d;
    }

    /**
     * map number to order direction, 0: OUTGOING (downstream), 1: INCOMING
     * (upstream), 2:
     * BOTH
     *
     * @param n number for enum
     * @return OrderDirection
     */
    private OrderDirection num2OrderDir(long n) {
        OrderDirection d = OrderDirection.NONE;
        if (n == 0) {
            d = OrderDirection.ASC;
        } else if (n == 1) {
            d = OrderDirection.DESC;
        }
        return d;
    }

    /**
     * @param ignoredTypes list of strings which are ignored types
     * @return RelationshipType[]
     */
    private RelationshipType[] getValidRelationshipTypes(List<String> ignoredTypes) {
        // prepare the edge types and direction
        ArrayList<RelationshipType> allowedEdgeTypes = new ArrayList<>();
        Iterable<RelationshipType> allEdgeTypes = (Iterable<RelationshipType>) getAllRelationshipTypes();

        for (RelationshipType r : allEdgeTypes) {
            String name = r.name();
            if (!ignoredTypes.contains(name)) {
                allowedEdgeTypes.add(r);
            }
        }
        return allowedEdgeTypes.toArray(new RelationshipType[0]);

    }

    /**
     * breadth-first search for graph of interest. keeps "fwd" and "rev" data for
     * each node and edge as "LabelData"
     *
     * @param nodeLabels   labels for nodes, should be persist to next calls
     * @param edgeLabels   labels for edges, should be persist to next calls
     * @param elementIds   initial nodes for search
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum length of a path between "elementIds"
     * @param dir          direction
     * @param isDirected   is directed?
     * @return graph elements visited
     */
    private BFSOutput GoI_BFS(HashMap<String, LabelData> nodeLabels, HashMap<String, LabelData> edgeLabels,
            HashSet<String> elementIds, List<String> ignoredTypes, long lengthLimit, Direction dir, boolean isDirected,
            boolean isFollowLabeled, TimeChecker timeChecker, HashSet<String> unavoidable) throws Exception {
        HashSet<String> nodeSet = new HashSet<>();
        HashSet<String> edgeSet = new HashSet<>();
        HashSet<String> visitedEdges = new HashSet<>();

        // prepare queue
        Queue<String> queue = new LinkedList<>(elementIds);

        RelationshipType[] allowedEdgeTypesArr = getValidRelationshipTypes(ignoredTypes);
        HashSet<String> ignoredTypesSet = new HashSet<>(ignoredTypes);
        Direction d = dir;
        if (!isDirected) {
            d = Direction.BOTH;
        }

        while (!queue.isEmpty()) {
            String n1 = queue.remove();

            Iterable<Relationship> edges = getNodeByElementId(n1).getRelationships(d, allowedEdgeTypesArr);
            timeChecker.checkTime();
            for (Relationship e : edges) {
                java.lang.String edgeId = e.getElementId();
                Node n2 = e.getOtherNode(getNodeByElementId(n1));
                String n2ElementId = n2.getElementId();
                LabelData labelE = edgeLabels.get(edgeId);
                boolean isIgnore = !elementIds.contains(n2ElementId) && !unavoidable.contains(n2ElementId)
                        && this.isNodeIgnored(n2, ignoredTypesSet);
                if (isIgnore || visitedEdges.contains(edgeId) ||
                        (isFollowLabeled && labelE == null)) {
                    continue;
                }
                visitedEdges.add(edgeId);

                if (labelE == null) {
                    labelE = new LabelData(lengthLimit + 1);
                }
                LabelData labelN1 = nodeLabels.get(n1);
                if (labelN1 == null) {
                    labelN1 = new LabelData(lengthLimit + 1);
                }
                if (dir == Direction.OUTGOING) {
                    labelE.fwd = labelN1.fwd + 1;
                } else if (dir == Direction.INCOMING) {
                    labelE.rev = labelN1.rev;
                }
                edgeLabels.put(edgeId, labelE);
                nodeLabels.put(n1, labelN1);

                nodeSet.add(n2ElementId);
                edgeSet.add(edgeId);
                LabelData labelN2 = nodeLabels.get(n2ElementId);
                if (labelN2 == null) {
                    labelN2 = new LabelData(lengthLimit + 1);
                }

                if (labelN2.getLabel(dir) > labelN1.getLabel(dir) + 1) {
                    labelN2.setLabel(labelN1.getLabel(dir) + 1, dir);
                    if (labelN2.getLabel(dir) < lengthLimit && !elementIds.contains(n2ElementId)) {
                        queue.add(n2ElementId);
                    }
                }
                nodeLabels.put(n2ElementId, labelN2);
                timeChecker.checkTime();
            }
        }
        return new BFSOutput(nodeSet, edgeSet);
    }

    /**
     * @param n            node
     * @param ignoredTypes list of strings which are ignored types
     * @return boolean
     */
    private boolean isNodeIgnored(Node n, HashSet<String> ignoredTypes) {
        for (Label l : n.getLabels()) {
            if (ignoredTypes.contains(l.name())) {
                return true;
            }
        }
        return false;
    }

    /**
     * chops all the nodes that are connected to only 1 node iteratively, returns a
     * graph where each node is
     * at least degree-2 or inside the list of srcIds
     *
     * @param srcElementIds element ids of nodes which are sources
     * @param subGraph      current sub-graph which will be modified
     */
    private void purify(HashSet<String> srcElementIds, BFSOutput subGraph) {
        HashMap<String, HashSet<String>> node2edge = new HashMap<>();
        HashMap<String, HashSet<String>> node2node = new HashMap<>();
        subGraph.nodes.addAll(srcElementIds);
        for (String edgeElementId : subGraph.edges) {
            Relationship r = getRelationshipByElementId(edgeElementId);
            String elementId1 = r.getStartNode().getElementId();
            String elementId2 = r.getEndNode().getElementId();
            this.insert2AdjList(node2edge, elementId1, edgeElementId);
            this.insert2AdjList(node2edge, elementId2, edgeElementId);
            // do not consider self-loops
            if (elementId1 != elementId2) {
                this.insert2AdjList(node2node, elementId1, elementId2);
                this.insert2AdjList(node2node, elementId2, elementId1);
            }
        }

        HashSet<String> degree1Nodes = this.getOrphanNodes(node2node, srcElementIds);

        while (degree1Nodes.size() > 0) {
            for (String nodeElementId : degree1Nodes) {
                subGraph.nodes.remove(nodeElementId);
                subGraph.edges.removeAll(node2edge.get(nodeElementId));

                // decrement the degree of the other node (node on the other hand of currently
                // deleted orphan node)
                HashSet<String> otherNodeIds = node2node.get(nodeElementId);
                for (String elementId : otherNodeIds) {
                    node2node.get(elementId).remove(nodeElementId);
                }
                node2node.remove(nodeElementId);
            }

            degree1Nodes = this.getOrphanNodes(node2node, srcElementIds);
        }
    }

    private void insert2AdjList(HashMap<String, HashSet<String>> map, String key, String val) {
        HashSet<String> set = map.get(key);
        if (set == null) {
            set = new HashSet<>();
        }
        set.add(val);
        map.put(key, set);
    }

    /**
     * remove edges that does not have source or target
     *
     * @param elms output from GoI_BFS
     * @return output from GoI_BFS
     */
    private BFSOutput removeOrphanEdges(BFSOutput elms) {
        BFSOutput result = new BFSOutput(elms.nodes, new HashSet<>());

        for (String edgeElementId : elms.edges) {
            Relationship r = getRelationshipByElementId(edgeElementId);
            String s = r.getStartNode().getElementId();
            String e = r.getEndNode().getElementId();
            if (elms.nodes.contains(s) && elms.nodes.contains(e)) {
                result.edges.add(edgeElementId);
            }
        }
        return result;
    }

    private HashSet<String> getOrphanNodes(HashMap<String, HashSet<String>> node2node, HashSet<String> srcIds) {
        HashSet<String> orphanNodes = new HashSet<>();
        for (String k : node2node.keySet()) {
            if (!srcIds.contains(k) && node2node.get(k).size() == 1) {
                orphanNodes.add(k);
            }
        }
        return orphanNodes;
    }

    private void endMeasuringTime(String msg, long start) {
        long end = System.nanoTime();
        String s = "" + Math.round((end - start) / 1000000000.0 * 100) / 100.0;
        log.info("executed in " + s + " seconds for " + msg);
    }

    public static class Output {
        public List<Node> nodes;
        public long totalNodeCount;
        public List<String> nodeClass;
        public List<String> nodeElementId;

        public List<Relationship> edges;
        public List<String> edgeClass;
        public List<String> edgeElementId;
        public List<List<String>> edgeSourceTargets;

        Output() {
            this.nodes = new ArrayList<>();
            this.edges = new ArrayList<>();
            this.nodeClass = new ArrayList<>();
            this.edgeClass = new ArrayList<>();
            this.nodeElementId = new ArrayList<>();
            this.edgeElementId = new ArrayList<>();
            this.edgeSourceTargets = new ArrayList<>();
        }
    }

    public static class CommonStreamOutput {
        public List<String> targetRegulatorNodeElementIds;
        public List<Node> nodes;
        public long totalNodeCount;
        public List<String> nodeClass;
        public List<String> nodeElementId;

        public List<Relationship> edges;
        public List<String> edgeClass;
        public List<String> edgeElementId;
        public List<List<String>> edgeSourceTargets;

        CommonStreamOutput(Output o, List<String> targetRegulatorNodeElementIds) {
            this.nodes = o.nodes;
            this.edges = o.edges;
            this.nodeClass = o.nodeClass;
            this.edgeClass = o.edgeClass;
            this.nodeElementId = o.nodeElementId;
            this.edgeElementId = o.edgeElementId;
            this.totalNodeCount = o.totalNodeCount;
            this.edgeSourceTargets = o.edgeSourceTargets;
            this.targetRegulatorNodeElementIds = targetRegulatorNodeElementIds;
        }
    }

    public static class BFSOutput {
        public HashSet<String> nodes;
        public HashSet<String> edges;

        BFSOutput(HashSet<String> nodes, HashSet<String> edges) {
            this.nodes = nodes;
            this.edges = edges;
        }
    }

    public static class CSOutput {
        public HashSet<String> nodes;
        public HashSet<String> edges;
        public HashSet<String> targetRegulatorNodes;

        CSOutput(HashSet<String> nodes, HashSet<String> edges, HashSet<String> targetRegulatorNodes) {
            this.nodes = nodes;
            this.edges = edges;
            this.targetRegulatorNodes = targetRegulatorNodes;
        }
    }

    public static class LabelData {
        public long fwd;
        public long rev;

        LabelData(long fwd, long rev) {
            this.fwd = fwd;
            this.rev = rev;
        }

        LabelData(long n) {
            this.fwd = n;
            this.rev = n;
        }

        public void setLabel(long val, Direction dir) {
            if (dir == Direction.INCOMING) {
                this.rev = val;
            } else if (dir == Direction.OUTGOING) {
                this.fwd = val;
            }
        }

        public long getLabel(Direction dir) {
            if (dir == Direction.INCOMING) {
                return this.rev;
            }
            if (dir == Direction.OUTGOING) {
                return this.fwd;
            }
            return -1;
        }
    }

    public static class TimeChecker {
        private final long _startTime; // in nano seconds
        private final long _timeout; // in milliseconds

        /**
         * @param timeout maximum allowed duration in milliseconds
         */
        public TimeChecker(long timeout) {
            this._startTime = System.nanoTime();
            this._timeout = timeout;
        }

        public void checkTime() throws Exception {
            long curr = System.nanoTime();
            long diff = (curr - this._startTime) / 1000000;
            if (diff > this._timeout) {
                throw new Exception("Timeout occurred! It takes longer than " + this._timeout + " milliseconds");
            }
        }
    }

    public enum OrderDirection {
        // ascending
        ASC,
        // descending
        DESC,
        // no ordering
        NONE
    }

}