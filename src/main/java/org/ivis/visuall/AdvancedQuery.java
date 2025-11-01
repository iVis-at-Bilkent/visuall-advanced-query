package org.ivis.visuall;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class AdvancedQuery {
    // ==== Contexts ====
    @Context public GraphDatabaseService db;
    @Context public Log log;
    @Context public Transaction tx;

    // ==== ElementId helpers ====
    public Relationship getRelationshipByElementId(String edgeElementId) {
        return tx.getRelationshipByElementId(edgeElementId);
    }
    public Node getNodeByElementId(String nodeElementId) {
        return tx.getNodeByElementId(nodeElementId);
    }
    public Iterable<RelationshipType> getAllRelationshipTypes() {
        return tx.getAllRelationshipTypes();
    }

    // ============================================================
    // =============== UNIFIED 0–1 BFS ENGINE =====================
    // ============================================================

    private static final RelationshipType BTC = RelationshipType.withName("belongs_to_complex");

    private static class BfsResult {
        Map<String,Integer> bestCost = new HashMap<>();
        Map<String,String>  prevNode = new HashMap<>();
        Map<String,String>  prevRel  = new HashMap<>();
    }

    /** Unified step cost used by every refactored procedure. */
    private int edgeCost(Relationship rel, Node to) {
        if (BTC.name().equals(rel.getType().name())) return 0;  // membership is always 0-cost
        return isProcessNode(to) ? 0 : 1;                        // real edges: 0 if 'to' is process, else 1
    }

    private Node safeGetNode(String elementId) {
        try { return tx.getNodeByElementId(elementId); } catch (Exception e) { return null; }
    }
    private Relationship safeGetRel(String elementId) {
        try { return tx.getRelationshipByElementId(elementId); } catch (Exception e) { return null; }
    }

    /**
     * 0–1 BFS from one or many sources. Real edges follow realEdgeDir and honor ignoredEdgeTypes (except BTC which is
     * always traversed in BOTH w/ 0-cost). A node is traversable only if allowNode.test(node) is true.
     */
    private BfsResult bfs01WithPrev(
            Iterable<String> startIds,
            long budget,
            Direction realEdgeDir,
            Set<String> ignoredEdgeTypes,
            Predicate<Node> allowNode,
            TimeChecker tc) {

        BfsResult res = new BfsResult();
        ArrayDeque<String> dq = new ArrayDeque<>();

        for (String s : startIds) {
            Node n = safeGetNode(s);
            if (n == null) continue;
            res.bestCost.put(s, 0);
            dq.addFirst(s);
        }

        while (!dq.isEmpty()) {
            String currId = dq.removeFirst();
            Node curr = safeGetNode(currId);
            if (curr == null) continue;
            int currCost = res.bestCost.get(currId);
            if (currCost > budget) continue;

            // ---- Real edges in requested direction (skip BTC here; handle below) ----
            for (Relationship rel : curr.getRelationships(realEdgeDir)) {
                String t = rel.getType().name();
                if (BTC.name().equals(t)) continue;                    // BTC handled separately
                if (ignoredEdgeTypes != null && ignoredEdgeTypes.contains(t)) continue;

                Node nb = rel.getOtherNode(curr);
                if (allowNode != null && !allowNode.test(nb)) continue;

                String nbId = nb.getElementId();
                int step = edgeCost(rel, nb);
                int next = currCost + step;
                if (next > budget) continue;

                Integer seen = res.bestCost.get(nbId);
                if (seen != null && seen <= next) continue;

                res.bestCost.put(nbId, next);
                res.prevNode.put(nbId, currId);
                res.prevRel.put(nbId, rel.getElementId());
                if (step == 0) dq.addFirst(nbId); else dq.addLast(nbId);
            }

            // ---- Membership edges (always BOTH, 0-cost) ----
            for (Relationship r : curr.getRelationships(Direction.BOTH, BTC)) {
                Node nb = r.getOtherNode(curr);
                if (allowNode != null && !allowNode.test(nb)) continue;

                String nbId = nb.getElementId();
                int next = currCost; // 0-cost
                Integer seen = res.bestCost.get(nbId);
                if (seen != null && seen <= next) continue;

                res.bestCost.put(nbId, next);
                res.prevNode.put(nbId, currId);
                res.prevRel.put(nbId, r.getElementId());
                dq.addFirst(nbId);
            }

            if (tc != null) {
                try { tc.checkTime(); } catch (Exception ignored) { /* let outer code throw if desired */ }
            }
        }
        return res;
    }

    private BfsResult bfs01WithPrev(String startId,
                                    long budget,
                                    Direction realEdgeDir,
                                    Set<String> ignoredEdgeTypes,
                                    Predicate<Node> allowNode) {
        List<String> singleton = Collections.singletonList(startId);
        return bfs01WithPrev(singleton, budget, realEdgeDir, ignoredEdgeTypes, allowNode, null);
    }

    /** Reconstruct exact path src -> dst into keep sets using prev maps. */
    private void reconstructPath(String srcId, String dstId,
                                 BfsResult res,
                                 Set<String> keepNodes,
                                 Set<String> keepEdges) {
        String walk = dstId;
        while (walk != null && !walk.equals(srcId)) {
            keepNodes.add(walk);
            String eId = res.prevRel.get(walk);
            if (eId != null) keepEdges.add(eId);   // do not filter here; traversal already honored ignored-edge set
            walk = res.prevNode.get(walk);
        }
        keepNodes.add(srcId);
    }

    /** Expand membership closure (both directions) into the keep sets. */
    private void expandMembershipClosureIntoIds(Set<String> keepNodeIds, Set<String> keepEdgeIds) {
        ArrayDeque<String> q = new ArrayDeque<>();
        HashSet<String> seen = new HashSet<>();
        for (String id : new ArrayList<>(keepNodeIds)) if (seen.add(id)) q.add(id);

        while (!q.isEmpty()) {
            String xId = q.removeFirst();
            Node x;
            try { x = tx.getNodeByElementId(xId); } catch (Exception e) { continue; }
            if (x == null) continue;

            for (Relationship r : x.getRelationships(Direction.BOTH, BTC)) {
                String eId = r.getElementId();
                Node y = r.getOtherNode(x);
                String yId = y.getElementId();

                keepEdgeIds.add(eId);
                if (keepNodeIds.add(yId) && seen.add(yId)) {
                    q.addLast(yId);
                }
            }
        }
    }

    // ============================================================
    // ===================== PROCEDURES ===========================
    // ============================================================

    // ------------------------ Graph of Interest ------------------------
    @Procedure(value = "graphOfInterest", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> graphOfInterest(
            @Name("elementIds") List<String> elementIds,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") long lengthLimit,
            @Name("isDirected") boolean isDirected,
            @Name("pageSize") long pageSize,
            @Name("currPage") long currPage,
            @Name("filterTxt") String filterTxt,
            @Name("isIgnoreCase") boolean isIgnoreCase,
            @Name("orderBy") String orderBy,
            @Name("orderDir") long orderDir,
            @Name("timeMapping") Map<String, List<String>> timeMapping,
            @Name("startTime") long startTime,
            @Name("endTime") long endTime,
            @Name("inclusionType") long inclusionType,
            @Name("timeout") long timeout,
            @Name("idFilter") List<String> idFilter) throws Exception {

        long executionStarted = System.nanoTime();
        TimeChecker timeChecker = new TimeChecker(timeout);
        BFSOutput o1 = GoI(elementIds, ignoredTypes, lengthLimit, isDirected, timeChecker);
        this.endMeasuringTime("Graph of interest", executionStarted);

        executionStarted = System.nanoTime();
        o1 = this.filterByDate(o1, startTime, endTime, timeMapping, inclusionType);

        // Preserve your existing behavior: add membership closure to keep sets
        expandMembershipClosureIntoIds(o1.nodes, o1.edges);

        Set<String> protectedNodesGoI = new HashSet<>(elementIds);
        Set<String> ignoredEdgeTypesGoI = new HashSet<>(ignoredTypes == null ? Collections.emptySet() : ignoredTypes);
        pruneProcessLeafTails(o1.nodes, o1.edges, protectedNodesGoI, ignoredEdgeTypesGoI);

        BFSOutput pageGraph = new BFSOutput(new HashSet<>(o1.nodes), new HashSet<>(o1.edges));
        pageGraph.nodes.removeAll(elementIds);

        this.endMeasuringTime("Filter by date", executionStarted);

        executionStarted = System.nanoTime();
        int cntSrcNode = elementIds.size();
        int cntSkip = Math.max(0, (int) ((currPage - 1) * pageSize) - cntSrcNode);
        long numSrcNode2return = Math.min(pageSize, Math.max(0, cntSrcNode - (currPage - 1) * pageSize));
        Output o2;
        if (idFilter == null) {
            o2 = this.tableFiltering(pageGraph, pageSize - numSrcNode2return, cntSkip, filterTxt, isIgnoreCase, orderBy, orderDir);
            o2.totalNodeCount += cntSrcNode;
        } else {
            idFilter.addAll(elementIds);
            o2 = this.idFiltering(pageGraph, idFilter);
        }
        this.endMeasuringTime("Filter by date", executionStarted);

        if (numSrcNode2return > 0) {
            int fromIdx = Math.max(0, (int) ((currPage - 1) * pageSize));
            int toIdx = Math.min(cntSrcNode, (int) (currPage * pageSize));
            this.addSourceNodes(o2, elementIds.subList(fromIdx, toIdx));
        }
        expandComplexMembersInOutput(o2);
        return Stream.of(o2);
    }

    // ------------------------ Common Stream ------------------------
    @Procedure(value = "commonStream", mode = Mode.WRITE)
    @Description("From specified nodes forms founds common upstream/downstream (target/regulator) with unified hop cost")
    public Stream<CommonStreamOutput> commonStream(
            @Name("elementIds") List<String> elementIds,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") long lengthLimit,
            @Name("direction") long direction,
            @Name("pageSize") long pageSize,
            @Name("currPage") long currPage,
            @Name("filterTxt") String filterTxt,
            @Name("isIgnoreCase") boolean isIgnoreCase,
            @Name("orderBy") String orderBy,
            @Name("orderDir") long orderDir,
            @Name("timeMapping") Map<String, List<String>> timeMapping,
            @Name("startTime") long startTime,
            @Name("endTime") long endTime,
            @Name("inclusionType") long inclusionType,
            @Name("timeout") long timeout,
            @Name("idFilter") List<String> idFilter) throws Exception {

        long executionStarted = System.nanoTime();
        Direction realDir = num2Dir(direction);
        Set<String> ignored = (ignoredTypes == null) ? Collections.emptySet() : new HashSet<>(ignoredTypes);

        if (elementIds == null || elementIds.size() < 2) {
            Output o = new Output();
            return Stream.of(new CommonStreamOutput(o, Collections.emptyList()));
        }

        // 1) BFS from each source
        List<BfsResult> perSource = new ArrayList<>(elementIds.size());
        for (String src : elementIds) {
            perSource.add(bfs01WithPrev(Collections.singletonList(src), lengthLimit, realDir, ignored, n -> true, null));
        }
        this.endMeasuringTime("Common stream (BFS per source)", executionStarted);

        // 2) Intersection of reachable
        Set<String> common = new HashSet<>(perSource.get(0).bestCost.keySet());
        for (int i = 1; i < perSource.size(); i++) common.retainAll(perSource.get(i).bestCost.keySet());
        common.removeAll(elementIds);

        // 3) Reconstruct exact paths
        Set<String> keepNodes = new HashSet<>(elementIds);
        Set<String> keepEdges = new HashSet<>();
        for (String t : common) {
            for (int i = 0; i < elementIds.size(); i++) {
                if (perSource.get(i).bestCost.containsKey(t)) {
                    reconstructPath(elementIds.get(i), t, perSource.get(i), keepNodes, keepEdges);
                }
            }
        }

        // Membership closure (kept behavior) + prune
        expandMembershipClosureIntoIds(keepNodes, keepEdges);

        Set<String> protectedNodes = new HashSet<>(elementIds);
        for (String t : common) {
            Node tn = getNodeByElementId(t);
            if (tn != null && !isProcessNode(tn)) protectedNodes.add(t);
        }
        pruneProcessLeafTails(keepNodes, keepEdges, protectedNodes, new HashSet<>(ignored));

        // 4) Date filter → prune again → filtering/pagination
        BFSOutput base = new BFSOutput(new HashSet<>(keepNodes), new HashSet<>(keepEdges));

        long t0 = System.nanoTime();
        BFSOutput filtered = this.filterByDate(base, startTime, endTime, timeMapping, inclusionType);
        pruneProcessLeafTails(filtered.nodes, filtered.edges, protectedNodes, new HashSet<>(ignored));
        this.endMeasuringTime("Common stream (filter+prune)", t0);

        // 5) Pagination graph: now drop seeds only from nodes (edges stay intact)
        BFSOutput pageGraph = new BFSOutput(new HashSet<>(filtered.nodes), new HashSet<>(filtered.edges));
        pageGraph.nodes.removeAll(elementIds);
        
        int cntSrcNode = elementIds.size();
        int cntSkip = Math.max(0, (int) ((currPage - 1) * pageSize) - cntSrcNode);
        long numSrcNode2return = Math.min(pageSize, Math.max(0, cntSrcNode - (currPage - 1) * pageSize));

        Output o2;
        if (idFilter == null) {
            o2 = this.tableFiltering(pageGraph, pageSize - numSrcNode2return, cntSkip, filterTxt, isIgnoreCase, orderBy, orderDir);
            o2.totalNodeCount += cntSrcNode;
        } else {
            idFilter.addAll(elementIds);
            o2 = this.idFiltering(pageGraph, idFilter);
        }

        if (numSrcNode2return > 0) {
            int fromIdx = Math.max(0, (int) ((currPage - 1) * pageSize));
            int toIdx = Math.min(cntSrcNode, (int) (currPage * pageSize));
            this.addSourceNodes(o2, elementIds.subList(fromIdx, toIdx));
        }
        expandComplexMembersInOutput(o2);
        return Stream.of(new CommonStreamOutput(o2, new ArrayList<>(common)));
    }

    // ------------------------ Neighborhood (elementIds) ------------------------
    @Procedure(value = "neighborhood", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> neighborhood(
            @Name("elementIds") List<String> elementIds,
            @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") long lengthLimit,
            @Name("isDirected") boolean isDirected,
            @Name("pageSize") long pageSize,
            @Name("currPage") long currPage,
            @Name("filterTxt") String filterTxt,
            @Name("isIgnoreCase") boolean isIgnoreCase,
            @Name("orderBy") String orderBy,
            @Name("orderDir") long orderDir,
            @Name("timeMapping") Map<String, List<String>> timeMapping,
            @Name("startTime") long startTime,
            @Name("endTime") long endTime,
            @Name("inclusionType") long inclusionType,
            @Name("timeout") long timeout,
            @Name("idFilter") List<String> idFilter) throws Exception {

        long executionStarted = System.nanoTime();
        Direction realDir = isDirected ? Direction.OUTGOING : Direction.BOTH;
        Set<String> ignored = (ignoredTypes == null) ? Collections.emptySet() : new HashSet<>(ignoredTypes);

        // Multi-source BFS; record prev; union a minimal forest
        BfsResult res = bfs01WithPrev(elementIds, lengthLimit, realDir, ignored, n -> true, new TimeChecker(timeout));

        Set<String> keepNodes = new HashSet<>();
        Set<String> keepEdges = new HashSet<>();

        // Build a forest from prev edges; exclude seeds from node set (added later via pagination)
        HashSet<String> srcSet = new HashSet<>(elementIds);
        for (Map.Entry<String, String> e : res.prevNode.entrySet()) {
            String nbId = e.getKey();
            String prev = e.getValue();
            String relId = res.prevRel.get(nbId);
            if (relId != null) keepEdges.add(relId);
            if (!srcSet.contains(nbId)) keepNodes.add(nbId);
            keepNodes.add(prev);
        }

        // Date filter, membership closure (as before), prune
        BFSOutput o1 = new BFSOutput(new HashSet<>(keepNodes), new HashSet<>(keepEdges));
        o1 = this.filterByDate(o1, startTime, endTime, timeMapping, inclusionType);

        expandMembershipClosureIntoIds(o1.nodes, o1.edges);
        Set<String> protectedNodesNh = new HashSet<>(elementIds);
        Set<String> ignoredEdgeTypesNh = new HashSet<>(ignored);
        pruneProcessLeafTails(o1.nodes, o1.edges, protectedNodesNh, ignoredEdgeTypesNh);

        this.endMeasuringTime("neighborhood", executionStarted);

        // Table/id filter & pagination
        executionStarted = System.nanoTime();
        int cntSrcNode = elementIds.size();
        int cntSkip = Math.max(0, (int) ((currPage - 1) * pageSize) - cntSrcNode);
        long numSrcNode2return = Math.min(pageSize, Math.max(0, cntSrcNode - (currPage - 1) * pageSize));

        Output o2;
        if (idFilter == null) {
            o2 = this.tableFiltering(o1, pageSize - numSrcNode2return, cntSkip, filterTxt, isIgnoreCase, orderBy, orderDir);
            o2.totalNodeCount += cntSrcNode;
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
        expandComplexMembersInOutput(o2);
        return Stream.of(o2);
    }

    // ------------------------ pathsBetween (internal ids) ------------------------
    @Procedure(value = "pathsBetween", mode = Mode.READ)
    @Description("pathsBetween(idList, lengthLimit, cloningThreshold): " +
            "Undirected paths between any pair of given INTERNAL node ids, " +
            "blocking high-degree simple_chemical nodes, " +
            "traversing membership edges at 0-cost, and counting only non-process steps.")
    public Stream<PathsBetweenOutput> pathsBetween(
            @Name("idList") List<Long> idList,
            @Name("lengthLimit") long lengthLimit,
            @Name("cloningThreshold") long cloningThreshold) {

        if (idList == null || idList.size() < 2 || lengthLimit < 0) {
            return Stream.of(new PathsBetweenOutput(Collections.emptyList(), Collections.emptyList(), "HybridAny"));
        }

        // Resolve seeds (skip blocked simple_chemical)
        List<Node> seeds = new ArrayList<>();
        for (Long id : idList) {
            if (id == null) continue;
            Node n = getNodeByInternalId(id);
            if (n != null && !isBlockedSimpleChemical(n, cloningThreshold)) seeds.add(n);
        }
        if (seeds.size() < 2) {
            return Stream.of(new PathsBetweenOutput(Collections.emptyList(), Collections.emptyList(), "HybridAny"));
        }

        Set<String> ignored = new HashSet<>(Arrays.asList("belongs_to_compartment", "belongs_to_submap"));
        Direction realDir = Direction.BOTH;

        // Precompute BFS per source (reuse for all pairs)
        Map<String, BfsResult> bySrc = new HashMap<>();
        Predicate<Node> allowNode = nb -> !isBlockedSimpleChemical(nb, cloningThreshold);
        for (Node src : seeds) {
            String srcId = src.getElementId();
            bySrc.put(srcId, bfs01WithPrev(Collections.singletonList(srcId), lengthLimit, realDir, ignored, allowNode, null));
        }

        Set<String> keepNodes = new HashSet<>();
        Set<String> keepEdges = new HashSet<>();

        for (int i = 0; i < seeds.size(); i++) {
            String srcId = seeds.get(i).getElementId();
            BfsResult res = bySrc.get(srcId);
            if (res == null) continue;

            for (int j = i + 1; j < seeds.size(); j++) {
                String dstId = seeds.get(j).getElementId();
                Integer cost = res.bestCost.get(dstId);
                if (cost == null || cost > lengthLimit) continue;

                reconstructPath(srcId, dstId, res, keepNodes, keepEdges);
            }
        }

        // Optional: membership closure on result (keep old behavior minimal: path edges only)
        // expandMembershipClosureIntoIds(keepNodes, keepEdges);

        // Prune process-only tails, protect seed endpoints
        Set<String> protectedNodes = new HashSet<>();
        for (Node n : seeds) protectedNodes.add(n.getElementId());
        pruneProcessLeafTails(keepNodes, keepEdges, protectedNodes, ignored);

        // Materialize
        List<Node> outNodes = new ArrayList<>(keepNodes.size());
        for (String id : keepNodes) outNodes.add(getNodeByElementId(id));

        List<Relationship> outRels = new ArrayList<>(keepEdges.size());
        for (String id : keepEdges) outRels.add(getRelationshipByElementId(id));

        Set<String> langs = new HashSet<>();
        for (Node n : outNodes) if (n.hasProperty("language")) {
            Object lang = n.getProperty("language");
            if (lang != null) langs.add(String.valueOf(lang));
        }
        String language = (langs.size() == 1) ? langs.iterator().next() : "HybridAny";
        return Stream.of(new PathsBetweenOutput(outNodes, outRels, language));
    }

    // ------------------------ pathsFromTo (internal ids) ------------------------
    @Procedure(value = "pathsFromTo", mode = Mode.READ)
    @Description("pathsFromTo(idList, lengthLimit, simpleChemicalDegreeThreshold): " +
            "Undirected paths between any two INTERNAL ids, unifying hop cost and membership traversal.")
    public Stream<PathsBetweenOutput> pathsFromTo(
            @Name("idList") List<Long> idList,
            @Name("lengthLimit") long lengthLimit,
            @Name("simpleChemicalDegreeThreshold") long simpleChemDegThreshold) {

        if (idList == null || idList.size() < 2 || lengthLimit < 0) {
            return Stream.of(new PathsBetweenOutput(Collections.emptyList(), Collections.emptyList(), "HybridAny"));
        }

        List<Node> seeds = new ArrayList<>();
        for (Long id : idList) {
            if (id == null) continue;
            Node n = getNodeByInternalId(id);
            if (n != null && !isBlockedSimpleChemical(n, simpleChemDegThreshold)) seeds.add(n);
        }
        if (seeds.size() < 2) {
            return Stream.of(new PathsBetweenOutput(Collections.emptyList(), Collections.emptyList(), "HybridAny"));
        }

        Set<String> ignored = new HashSet<>(Arrays.asList("belongs_to_compartment", "belongs_to_submap"));
        Direction realDir = Direction.BOTH;

        Map<String, BfsResult> bySrc = new HashMap<>();
        Predicate<Node> allowNode = nb -> !isBlockedSimpleChemical(nb, simpleChemDegThreshold);
        for (Node src : seeds) {
            String srcId = src.getElementId();
            bySrc.put(srcId, bfs01WithPrev(Collections.singletonList(srcId), lengthLimit, realDir, ignored, allowNode, null));
        }

        Set<String> keepNodes = new HashSet<>();
        Set<String> keepEdges = new HashSet<>();

        for (int i = 0; i < seeds.size(); i++) {
            String srcId = seeds.get(i).getElementId();
            BfsResult res = bySrc.get(srcId);
            if (res == null) continue;

            for (int j = i + 1; j < seeds.size(); j++) {
                String dstId = seeds.get(j).getElementId();
                Integer cost = res.bestCost.get(dstId);
                if (cost == null || cost > lengthLimit) continue;

                reconstructPath(srcId, dstId, res, keepNodes, keepEdges);
            }
        }

        Set<String> protectedNodes = new HashSet<>();
        for (Node n : seeds) protectedNodes.add(n.getElementId());
        pruneProcessLeafTails(keepNodes, keepEdges, protectedNodes, ignored);

        List<Node> outNodes = new ArrayList<>(keepNodes.size());
        for (String id : keepNodes) outNodes.add(getNodeByElementId(id));
        List<Relationship> outRels = new ArrayList<>(keepEdges.size());
        for (String id : keepEdges) outRels.add(getRelationshipByElementId(id));

        Set<String> langs = new HashSet<>();
        for (Node n : outNodes) if (n.hasProperty("language")) {
            Object v = n.getProperty("language");
            if (v != null) langs.add(String.valueOf(v));
        }
        String language = (langs.size() == 1) ? langs.iterator().next() : "HybridAny";
        return Stream.of(new PathsBetweenOutput(outNodes, outRels, language));
    }

    // ------------------------ neighborhoodFromIds (internal ids) ------------------------
    @Procedure(value = "neighborhoodFromIds", mode = Mode.READ)
    @Description("neighborhoodFromIds(idList, lengthLimit, simpleChemicalDegreeThreshold): " +
            "Undirected neighborhood from INTERNAL ids with unified hop cost (membership 0-cost, real edges 0 to process else 1); " +
            "blocks high-degree simple_chemical nodes.")
    public Stream<PathsBetweenOutput> neighborhoodFromIds(
            @Name("idList") List<Long> idList,
            @Name("lengthLimit") long lengthLimit,
            @Name("simpleChemicalDegreeThreshold") long simpleChemDegThreshold) {

        if (idList == null || idList.isEmpty() || lengthLimit < 0) {
            return Stream.of(new PathsBetweenOutput(Collections.emptyList(), Collections.emptyList(), "HybridAny"));
        }

        List<Node> seeds = new ArrayList<>();
        for (Long id : idList) {
            if (id == null) continue;
            Node n = getNodeByInternalId(id);
            if (n != null && !isBlockedSimpleChemical(n, simpleChemDegThreshold)) seeds.add(n);
        }
        if (seeds.isEmpty()) {
            return Stream.of(new PathsBetweenOutput(Collections.emptyList(), Collections.emptyList(), "HybridAny"));
        }

        Set<String> ignored = new HashSet<>(Arrays.asList("belongs_to_compartment", "belongs_to_submap"));
        Direction realDir = Direction.BOTH;
        Predicate<Node> allowNode = nb -> !isBlockedSimpleChemical(nb, simpleChemDegThreshold);

        List<String> startIds = new ArrayList<>();
        for (Node n : seeds) startIds.add(n.getElementId());
        BfsResult res = bfs01WithPrev(startIds, lengthLimit, realDir, ignored, allowNode, null);

        // Build forest from prev
        Set<String> keepNodes = new HashSet<>();
        Set<String> keepEdges = new HashSet<>();
        HashSet<String> srcSet = new HashSet<>(startIds);
        for (Map.Entry<String, String> e : res.prevNode.entrySet()) {
            String nbId = e.getKey();
            String prev = e.getValue();
            String relId = res.prevRel.get(nbId);
            if (relId != null) keepEdges.add(relId);
            if (!srcSet.contains(nbId)) keepNodes.add(nbId);
            keepNodes.add(prev);
        }

        // Optionally expand membership in output
        // expandMembershipClosureIntoIds(keepNodes, keepEdges);

        // Prune protecting seeds
        Set<String> protectedNodes = new HashSet<>(srcSet);
        pruneProcessLeafTails(keepNodes, keepEdges, protectedNodes, ignored);

        List<Node> outNodes = new ArrayList<>(keepNodes.size());
        for (String id : keepNodes) outNodes.add(getNodeByElementId(id));
        List<Relationship> outRels = new ArrayList<>(keepEdges.size());
        for (String id : keepEdges) outRels.add(getRelationshipByElementId(id));

        Set<String> langs = new HashSet<>();
        for (Node n : outNodes) if (n.hasProperty("language")) {
            Object v = n.getProperty("language");
            if (v != null) langs.add(String.valueOf(v));
        }
        String language = (langs.size() == 1) ? langs.iterator().next() : "HybridAny";
        return Stream.of(new PathsBetweenOutput(outNodes, outRels, language));
    }

    // ------------------------ Error probe ------------------------
    @Procedure(value = "error", mode = Mode.WRITE)
    @Description("testing for error")
    public Stream<Output> error() {
        throw new RuntimeException("Error testing 123");
    }

    // ============================================================
    // ==================== TABLE/ID FILTERS ======================
    // ============================================================

    private Output tableFiltering(BFSOutput o, long pageSize, int skip, String filterTxt, boolean isIgnoreCase,
                                  String orderBy, long orderDir) {
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);
        r.totalNodeCount = r.nodes.size();

        OrderDirection dir = num2OrderDir(orderDir);
        if (orderBy != null && dir != OrderDirection.NONE) {
            r.nodes.sort((n1, n2) -> {
                if (!n1.hasProperty(orderBy) || !n2.hasProperty(orderBy)) return 0;
                Object o1 = n1.getProperty(orderBy);
                Object o2 = n2.getProperty(orderBy);
                if (o1.getClass() == Integer.class) return ((Integer) o1).compareTo((Integer) o2);
                else if (o1.getClass() == String.class) return ((String) o1).compareTo((String) o2);
                else if (o1.getClass() == Double.class) return ((Double) o1).compareTo((Double) o2);
                else if (o1.getClass() == Float.class) return ((Float) o1).compareTo((Float) o2);
                return 0;
            });
            if (dir == OrderDirection.DESC) Collections.reverse(r.nodes);
        }

        int nodeCount = r.nodes.size();
        int fromIdx = Math.min(Math.max(0, skip), nodeCount);
        int toIdx = Math.min(fromIdx + (int) pageSize, nodeCount);
        r.nodes = r.nodes.subList(fromIdx, toIdx);

        // include all edges as provided (no type filter; matches prior behavior)
        for (String edgeElementId : o.edges) {
            Relationship e = getRelationshipByElementId(edgeElementId);
            r.edges.add(e);
            r.edgeClass.add(e.getType().name());
            r.edgeElementId.add(edgeElementId);
            String src = e.getStartNode().getElementId();
            String tgt = e.getEndNode().getElementId();
            ArrayList<String> l = new ArrayList<>();
            l.add(src); l.add(tgt);
            r.edgeSourceTargets.add(l);
        }

        for (Node n : r.nodes) {
            r.nodeClass.add(n.getLabels().iterator().next().name());
            r.nodeElementId.add(n.getElementId());
        }

        expandComplexMembersInOutput(r);
        return r;
    }

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
            l.add(src); l.add(tgt);
            r.edgeSourceTargets.add(l);
        }
        r.totalNodeCount = r.nodes.size();

        expandComplexMembersInOutput(r);
        return r;
    }

    private Output filterByTxt(BFSOutput o, String filterTxt, boolean isIgnoreCase) {
        Output r = new Output();
        if (filterTxt != null && filterTxt.length() > 0) {
            if (isIgnoreCase) filterTxt = filterTxt.toLowerCase();
            for (String id : o.nodes) {
                Node n = getNodeByElementId(id);
                List<String> l = new ArrayList<>();
                for (Object p : n.getAllProperties().values()) {
                    if (p.getClass().isArray()) {
                        StringBuilder s = new StringBuilder();
                        for (Object o2 : ((Object[]) p)) s.append(o2.toString());
                        l.add(s.toString());
                    } else {
                        l.add(p.toString());
                    }
                }
                boolean isPassed = false;
                for (String s : l) {
                    if (isIgnoreCase ? s.toLowerCase().contains(filterTxt) : s.contains(filterTxt)) {
                        isPassed = true; break;
                    }
                }
                if (isPassed) r.nodes.add(n);
            }
        } else {
            for (String id : o.nodes) r.nodes.add(getNodeByElementId(id));
        }
        return r;
    }

    private BFSOutput filterByDate(BFSOutput o, long d1, long d2, Map<String, List<String>> timeMapping, long inclusionType) {
        if (d1 == d2) return o;
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
            if (o instanceof Long) start = (long) o;
            else if (o instanceof Double) start = ((Double) o).longValue();
        }
        if (has2) {
            Object o = e.getProperty(propEndName);
            if (o instanceof Long) end = (long) o;
            else if (o instanceof Double) end = ((Double) o).longValue();
        }

        if (inclusionType == 0 && start <= d2 && end >= d1) set.add(elementId);
        else if (inclusionType == 1 && d1 <= start && d2 >= end) set.add(elementId);
        else if (inclusionType == 2 && start <= d1 && end >= d2) set.add(elementId);
    }

    private void addSourceNodes(Output o, List<String> elementIds) {
        for (String elementId : elementIds) {
            Node n = getNodeByElementId(elementId);
            o.nodeElementId.add(elementId);
            o.nodes.add(n);
            o.nodeClass.add(n.getLabels().iterator().next().name());
        }
    }

    // ============================================================
    // =================== GoI (compat, unified cost) =============
    // ============================================================

    private boolean withinBudget(LabelData l, long L, boolean undirected) {
        if (l == null) return true;
        // Reaching from either pass is enough in both modes as in your earlier code
        return (l.fwd <= L) || (l.rev <= L);
    }

    private BFSOutput GoI(List<String> elementIds, List<String> ignoredTypes, long lengthLimit, boolean isDirected,
                          TimeChecker timeChecker) throws Exception {
        HashSet<String> elementIdSet = new HashSet<>(elementIds);
        HashMap<String, LabelData> edgeLabels = new HashMap<>();
        HashMap<String, LabelData> nodeLabels = new HashMap<>();
        for (String elementId : elementIds) {
            nodeLabels.put(elementId, new LabelData(0, 0));
        }

        BFSOutput o1 = this.GoI_BFS(nodeLabels, edgeLabels, elementIdSet, ignoredTypes, lengthLimit,
                Direction.OUTGOING, isDirected, false, timeChecker, elementIdSet);

        BFSOutput o2 = this.GoI_BFS(nodeLabels, edgeLabels, elementIdSet, ignoredTypes, lengthLimit,
                Direction.INCOMING, isDirected, false, timeChecker, elementIdSet);

        o1.edges.addAll(o2.edges);
        o1.nodes.addAll(o2.nodes);

        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        for (String edgeElementId : o1.edges) {
            LabelData le = edgeLabels.get(edgeElementId);
            if (le == null || le.fwd + le.rev <= lengthLimit) {
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
     * GoI_BFS rewritten to use unified hop cost:
     * - real edges follow dir (or BOTH if !isDirected) and ignore types in ignoredTypes
     * - membership edges (BTC) are always traversed BOTH with cost 0
     */
    private BFSOutput GoI_BFS(
            HashMap<String, LabelData> nodeLabels,
            HashMap<String, LabelData> edgeLabels,
            HashSet<String> elementIds,
            List<String> ignoredTypes,
            long lengthLimit,
            Direction dir,
            boolean isDirected,
            boolean isFollowLabeled,
            TimeChecker timeChecker,
            HashSet<String> unignorable
    ) throws Exception {
        HashSet<String> nodeSet = new HashSet<>();
        HashSet<String> edgeSet = new HashSet<>();
        HashSet<String> visitedEdges = new HashSet<>();

        Queue<String> queue = new LinkedList<>(elementIds);
        HashSet<String> ignoredTypesSet = new HashSet<>(ignoredTypes);
        Direction d = isDirected ? dir : Direction.BOTH;

        while (!queue.isEmpty()) {
            String n1 = queue.remove();
            Node currNode = getNodeByElementId(n1);

            LabelData guard = nodeLabels.getOrDefault(n1, new LabelData(lengthLimit + 1));
            long currLabel = guard.getLabel(dir);
            if (currLabel > lengthLimit) continue;

            // ---- Real edges in direction d ----
            for (Relationship rel : currNode.getRelationships(d)) {
                String t = rel.getType().name();
                if (BTC.name().equals(t)) continue;                       // handle BTC below
                if (ignoredTypesSet.contains(t)) continue;

                Node nb = rel.getOtherNode(currNode);
                String n2 = nb.getElementId();

                // Add edge once
                String eId = rel.getElementId();
                if (!visitedEdges.contains(eId)) {
                    visitedEdges.add(eId);
                    edgeSet.add(eId);

                    LabelData le = edgeLabels.getOrDefault(eId, new LabelData(lengthLimit + 1));
                    LabelData ln1 = nodeLabels.getOrDefault(n1, new LabelData(lengthLimit + 1));
                    int step = edgeCost(rel, nb);
                    if (dir == Direction.OUTGOING) le.fwd = Math.min(le.fwd, ln1.fwd + step);
                    else if (dir == Direction.INCOMING) le.rev = Math.min(le.rev, ln1.rev + step);
                    edgeLabels.put(eId, le);
                }

                nodeSet.add(n2);
                LabelData ln2 = nodeLabels.getOrDefault(n2, new LabelData(lengthLimit + 1));
                LabelData ln1 = nodeLabels.getOrDefault(n1, new LabelData(lengthLimit + 1));
                int step = edgeCost(rel, nb);
                long next = ln1.getLabel(dir) + step;

                if (next < ln2.getLabel(dir)) {
                    ln2.setLabel(next, dir);
                    if (next <= lengthLimit && !elementIds.contains(n2)) queue.add(n2);
                }
                nodeLabels.put(n2, ln2);
            }

            // ---- Membership edges BOTH, 0-cost ----
            for (Relationship r : currNode.getRelationships(Direction.BOTH, BTC)) {
                Node nb = r.getOtherNode(currNode);
                String n2 = nb.getElementId();

                // add BTC edge
                String eId = r.getElementId();
                if (!visitedEdges.contains(eId)) {
                    visitedEdges.add(eId);
                    edgeSet.add(eId);

                    LabelData le = edgeLabels.getOrDefault(eId, new LabelData(lengthLimit + 1));
                    LabelData ln1 = nodeLabels.getOrDefault(n1, new LabelData(lengthLimit + 1));
                    if (dir == Direction.OUTGOING) le.fwd = Math.min(le.fwd, ln1.fwd /* +0 */);
                    else if (dir == Direction.INCOMING) le.rev = Math.min(le.rev, ln1.rev /* +0 */);
                    edgeLabels.put(eId, le);
                }

                nodeSet.add(n2);
                LabelData ln2 = nodeLabels.getOrDefault(n2, new LabelData(lengthLimit + 1));
                LabelData ln1 = nodeLabels.getOrDefault(n1, new LabelData(lengthLimit + 1));
                long next = ln1.getLabel(dir); // +0

                if (next < ln2.getLabel(dir)) {
                    ln2.setLabel(next, dir);
                    if (next <= lengthLimit && !elementIds.contains(n2)) queue.add(n2);
                }
                nodeLabels.put(n2, ln2);
            }

            timeChecker.checkTime();
        }
        return new BFSOutput(nodeSet, edgeSet);
    }

    // ============================================================
    // ===================== Misc helpers =========================
    // ============================================================

    private Direction num2Dir(long n) {
        Direction d = Direction.BOTH;
        if (n == 0) d = Direction.OUTGOING;
        else if (n == 1) d = Direction.INCOMING;
        return d;
    }

    private OrderDirection num2OrderDir(long n) {
        OrderDirection d = OrderDirection.NONE;
        if (n == 0) d = OrderDirection.ASC;
        else if (n == 1) d = OrderDirection.DESC;
        return d;
    }

    private RelationshipType[] getValidRelationshipTypes(List<String> ignoredTypes) {
        ArrayList<RelationshipType> allowedEdgeTypes = new ArrayList<>();
        Iterable<RelationshipType> allEdgeTypes = getAllRelationshipTypes();
        for (RelationshipType r : allEdgeTypes) {
            String name = r.name();
            if (!ignoredTypes.contains(name)) {
                allowedEdgeTypes.add(r);
            }
        }
        return allowedEdgeTypes.toArray(new RelationshipType[0]);
    }

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

    private void purify(HashSet<String> srcElementIds, BFSOutput subGraph) {
        HashMap<String, HashSet<String>> node2edge = new HashMap<>();
        HashMap<String, HashSet<String>> node2node = new HashMap<>();
        subGraph.nodes.addAll(srcElementIds);
        for (String edgeElementId : subGraph.edges) {
            Relationship r = getRelationshipByElementId(edgeElementId);
            String elementId1 = r.getStartNode().getElementId();
            String elementId2 = r.getEndNode().getElementId();
            insert2AdjList(node2edge, elementId1, edgeElementId);
            insert2AdjList(node2edge, elementId2, edgeElementId);
            if (elementId1 != elementId2) {
                insert2AdjList(node2node, elementId1, elementId2);
                insert2AdjList(node2node, elementId2, elementId1);
            }
        }

        HashSet<String> degree1Nodes = getOrphanNodes(node2node, srcElementIds);
        while (!degree1Nodes.isEmpty()) {
            for (String nodeElementId : degree1Nodes) {
                subGraph.nodes.remove(nodeElementId);
                subGraph.edges.removeAll(node2edge.get(nodeElementId));
                HashSet<String> otherNodeIds = node2node.get(nodeElementId);
                for (String elementId : otherNodeIds) {
                    node2node.get(elementId).remove(nodeElementId);
                }
                node2node.remove(nodeElementId);
            }
            degree1Nodes = getOrphanNodes(node2node, srcElementIds);
        }
    }

    private void insert2AdjList(HashMap<String, HashSet<String>> map, String key, String val) {
        HashSet<String> set = map.get(key);
        if (set == null) set = new HashSet<>();
        set.add(val);
        map.put(key, set);
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

    private boolean isComplexNode(Node n) {
        if (n.hasProperty("class") && "complex".equals(n.getProperty("class"))) return true;
        for (Label label : n.getLabels()) if ("complex".equalsIgnoreCase(label.name())) return true;
        return false;
    }

    private void endMeasuringTime(String msg, long start) {
        long end = System.nanoTime();
        String s = "" + Math.round((end - start) / 1000000000.0 * 100) / 100.0;
        log.info("executed in " + s + " seconds for " + msg);
    }

    // ==== DTOs ====

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

    public static class PathsBetweenOutput {
        public List<Node> nodes;
        public List<Relationship> relationships;
        public String language;
        public PathsBetweenOutput(List<Node> nodes, List<Relationship> rels, String language) {
            this.nodes = nodes;
            this.relationships = rels;
            this.language = language;
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
        LabelData(long fwd, long rev) { this.fwd = fwd; this.rev = rev; }
        LabelData(long n) { this.fwd = n; this.rev = n; }
        public void setLabel(long val, Direction dir) {
            if (dir == Direction.INCOMING) this.rev = val;
            else if (dir == Direction.OUTGOING) this.fwd = val;
        }
        public long getLabel(Direction dir) {
            if (dir == Direction.INCOMING) return this.rev;
            if (dir == Direction.OUTGOING) return this.fwd;
            return -1;
        }
    }

    public static class TimeChecker {
        private final long _startTime; // in nano seconds
        private final long _timeout;   // in milli seconds
        public TimeChecker(long timeout) { this._startTime = System.nanoTime(); this._timeout = timeout; }
        public void checkTime() throws Exception {
            if (_timeout <= 0) return;
            long curr = System.nanoTime();
            long diff = (curr - this._startTime) / 1000000;
            if (diff > this._timeout) {
                throw new Exception("Timeout occurred! It takes longer than " + this._timeout + " milliseconds");
            }
        }
    }

    public enum OrderDirection { ASC, DESC, NONE }

    // ============================================================
    // ============== Domain-specific helpers (kept) ==============
    // ============================================================

    private boolean isBlockedSimpleChemical(Node n, long cloningThreshold) {
        String cls = n.hasProperty("class") ? String.valueOf(n.getProperty("class")) : "";
        if (!"simple_chemical".equals(cls)) return false;
        int deg = n.getDegree(Direction.BOTH);
        return deg > cloningThreshold;
    }

    private Node getNodeByInternalId(long internalId) {
        try (Result res = tx.execute("MATCH (n) WHERE id(n) = $id RETURN n",
                Collections.singletonMap("id", internalId))) {
            if (res.hasNext()) return (Node) res.next().get("n");
        }
        return null;
    }

    private boolean isProcessNode(Node n) {
        if (n.hasProperty("class")) {
            Object c = n.getProperty("class");
            if (c != null && "process".equals(String.valueOf(c))) return true;
        }
        if (n.hasProperty("category")) {
            Object c = n.getProperty("category");
            if (c != null && "process".equals(String.valueOf(c))) return true;
        }
        for (Label label : n.getLabels()) if ("process".equalsIgnoreCase(label.name())) return true;
        return false;
    }

    /** After assembling Output r, add direct members for every complex (kept original behavior). */
    private void expandComplexMembersInOutput(Output r) {
        Set<String> nodeIds = new HashSet<>(r.nodeElementId);
        Set<String> edgeIds = new HashSet<>(r.edgeElementId);

        List<Node> addNodes = new ArrayList<>();
        List<String> addNodeClasses = new ArrayList<>();
        List<String> addNodeIds = new ArrayList<>();

        List<Relationship> addEdges = new ArrayList<>();
        List<String> addEdgeClasses = new ArrayList<>();
        List<String> addEdgeIds = new ArrayList<>();
        List<List<String>> addEdgeSrcTgt = new ArrayList<>();

        for (Node n : r.nodes) {
            if (isComplexNode(n)) {
                for (Relationship memberRel : n.getRelationships(Direction.INCOMING, BTC)) {
                    Node member = memberRel.getStartNode();
                    String mId = member.getElementId();
                    if (nodeIds.add(mId)) {
                        addNodes.add(member);
                        addNodeClasses.add(member.getLabels().iterator().next().name());
                        addNodeIds.add(mId);
                    }
                    String eId = memberRel.getElementId();
                    if (edgeIds.add(eId)) {
                        addEdges.add(memberRel);
                        addEdgeClasses.add(memberRel.getType().name());
                        addEdgeIds.add(eId);
                        ArrayList<String> st = new ArrayList<>(2);
                        st.add(memberRel.getStartNode().getElementId());
                        st.add(memberRel.getEndNode().getElementId());
                        addEdgeSrcTgt.add(st);
                    }
                }
            }
        }

        r.nodes.addAll(addNodes);
        r.nodeClass.addAll(addNodeClasses);
        r.nodeElementId.addAll(addNodeIds);

        r.edges.addAll(addEdges);
        r.edgeClass.addAll(addEdgeClasses);
        r.edgeElementId.addAll(addEdgeIds);
        r.edgeSourceTargets.addAll(addEdgeSrcTgt);
    }

    // Keep these legacy helpers for compatibility (not used by the new engine)
    private static final Set<String> REAL_BIO_EDGE_TYPES =
            new HashSet<>(Arrays.asList(
                    "consumption", "production", "modulation",
                    "stimulation", "necessary_stimulation",
                    "inhibition", "catalysis"
            ));

    private boolean hasRealEdge(Node a, Node b, Set<String> ignoredTypes) {
        for (Relationship rel : a.getRelationships(Direction.BOTH)) {
            if (rel.getOtherNode(a).equals(b)) {
                String t = rel.getType().name();
                if ((ignoredTypes == null || !ignoredTypes.contains(t)) && !t.startsWith("belongs_to_")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean shouldPruneProcessLeaf(String nodeId, Map<String, List<String>> incident) {
        List<String> inc = incident.getOrDefault(nodeId, Collections.emptyList());
        return inc.size() <= 1;
    }

    // Prune variants (kept, used by refactored code)
    private void pruneProcessLeafTails(BFSOutput o) { pruneProcessLeafTails(o.nodes, o.edges); }

    private void pruneProcessLeafTails(Set<String> nodeIds, Set<String> edgeIds) {
        Map<String, Integer> deg = new HashMap<>();
        Map<String, List<String>> incident = new HashMap<>();

        for (String eId : new ArrayList<>(edgeIds)) {
            Relationship r = getRelationshipByElementId(eId);
            String u = r.getStartNode().getElementId();
            String v = r.getEndNode().getElementId();
            if (!nodeIds.contains(u) || !nodeIds.contains(v)) continue;

            incident.computeIfAbsent(u, k -> new ArrayList<>()).add(eId);
            incident.computeIfAbsent(v, k -> new ArrayList<>()).add(eId);
            deg.put(u, deg.getOrDefault(u, 0) + 1);
            deg.put(v, deg.getOrDefault(v, 0) + 1);
        }

        ArrayDeque<String> q = new ArrayDeque<>();
        for (String nid : nodeIds) {
            Node n = getNodeByElementId(nid);
            boolean isProcess = isProcessNode(n);
            int d = deg.getOrDefault(nid, 0);
            if (isProcess && d <= 1 && shouldPruneProcessLeaf(nid, incident)) {
                q.add(nid);
            }
        }

        while (!q.isEmpty()) {
            String x = q.remove();
            if (!nodeIds.contains(x)) continue;
            Node nx = getNodeByElementId(x);
            if (!isProcessNode(nx)) continue;
            int dx = deg.getOrDefault(x, 0);
            if (dx > 1) continue;

            nodeIds.remove(x);
            List<String> inc = incident.getOrDefault(x, Collections.emptyList());
            for (String eId : inc) {
                if (!edgeIds.remove(eId)) continue;
                Relationship r = getRelationshipByElementId(eId);
                String u = r.getStartNode().getElementId();
                String v = r.getEndNode().getElementId();
                String y = u.equals(x) ? v : u;

                deg.put(x, Math.max(0, deg.getOrDefault(x, 0) - 1));
                deg.put(y, Math.max(0, deg.getOrDefault(y, 0) - 1));
                Node ny = getNodeByElementId(y);
                if (isProcessNode(ny) && deg.getOrDefault(y, 0) <= 1) q.add(y);
            }
        }
    }

    private void pruneProcessLeafTails(Set<String> nodeIds,
                                       Set<String> edgeIds,
                                       Set<String> protectedNodeIds,
                                       Set<String> ignoredEdgeTypes) {
        Map<String, List<String>> inc = new HashMap<>();
        for (String eId : new ArrayList<>(edgeIds)) {
            Relationship r;
            try { r = tx.getRelationshipByElementId(eId); } catch (Exception ex) { continue; }
            if (ignoredEdgeTypes != null && ignoredEdgeTypes.contains(r.getType().name())) continue;

            String u = r.getStartNode().getElementId();
            String v = r.getEndNode().getElementId();
            if (!nodeIds.contains(u) || !nodeIds.contains(v)) continue;

            inc.computeIfAbsent(u, k -> new ArrayList<>()).add(eId);
            inc.computeIfAbsent(v, k -> new ArrayList<>()).add(eId);
        }

        ArrayDeque<String> q = new ArrayDeque<>();
        for (String nid : new ArrayList<>(nodeIds)) {
            if (protectedNodeIds != null && protectedNodeIds.contains(nid)) continue;
            Node n;
            try { n = tx.getNodeByElementId(nid); } catch (Exception ex) { continue; }
            if (!isProcessNode(n)) continue;

            int deg = 0;
            for (String eId : inc.getOrDefault(nid, Collections.emptyList())) {
                if (edgeIds.contains(eId)) deg++;
            }
            if (deg <= 1) q.add(nid);
        }

        while (!q.isEmpty()) {
            String x = q.remove();
            if (!nodeIds.contains(x)) continue;
            if (protectedNodeIds != null && protectedNodeIds.contains(x)) continue;

            Node nx;
            try { nx = tx.getNodeByElementId(x); } catch (Exception ex) { continue; }
            if (!isProcessNode(nx)) continue;

            int degX = 0;
            for (String eId : inc.getOrDefault(x, Collections.emptyList())) {
                if (edgeIds.contains(eId)) degX++;
            }
            if (degX > 1) continue;

            nodeIds.remove(x);
            for (String eId : inc.getOrDefault(x, Collections.emptyList())) {
                if (!edgeIds.remove(eId)) continue;
                Relationship r;
                try { r = tx.getRelationshipByElementId(eId); } catch (Exception ex) { continue; }
                String u = r.getStartNode().getElementId();
                String v = r.getEndNode().getElementId();
                String y = u.equals(x) ? v : u;

                if (!nodeIds.contains(y)) continue;
                Node ny;
                try { ny = tx.getNodeByElementId(y); } catch (Exception ex) { continue; }
                if (!isProcessNode(ny)) continue;

                int degY = 0;
                for (String e2 : inc.getOrDefault(y, Collections.emptyList())) {
                    if (edgeIds.contains(e2)) degY++;
                }
                if (degY <= 1) q.add(y);
            }
        }

        edgeIds.removeIf(eId -> {
            try {
                Relationship r = tx.getRelationshipByElementId(eId);
                return !nodeIds.contains(r.getStartNode().getElementId())
                        || !nodeIds.contains(r.getEndNode().getElementId());
            } catch (Exception ignore) { return true; }
        });
    }
}
