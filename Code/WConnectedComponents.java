/**
 * Libraries && Packages importing in the class to make the build and compile of the code
 * be successful.
 * Importing neo4j driver packages and Collection data structures packages.
 */

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.*;

/**
 * Class that implements the Weakly Connected Components community detection algorithm.
 * Nodes are from the same cluster when they have any kind of relation from one Node to the other.
 * Outgoing and Ingoing relations are treated the same.
 *
 * [1] Implement the Weakly Connected Components graph algorithm on the Graph.
 *
 * ==> Code has been influenced from reference: https://www.geeksforgeeks.org/connected-components-in-an-undirected-graph/
 */
public class WConnectedComponents {

    /**
     * clusterSize = Holds the number of nodes involved in the cluster.
     * involved_nodes = Holds all the nodes that are involved in the query result. It is final
     *                  because the value of the nodes will never change (as a list).
     */
    private int clusterSize;
    private final List<Record> involved_nodes;
    private Map<Node, Map<Node, Relationship>> nodeRelations;
    private Map<Integer, Set<Node>> resultantSet;

    /**
     * Map that calculates how strongly connected a node is. The more relationships involved with a node
     * the more connected/popular it is.
     */
    private  ArrayList<Node> degreeCentrality;

    /**
     * Creating a StringBuilder to save space on Java printing and having more space to show important
     * results.
     */
    private StringBuilder community_printer;
    private int community_counter;

    /**
     * Constructor of the class. Values will be initialized here and the appropriate methods will be applied.
     */
    public WConnectedComponents(List<Record> involved_nodes,  Map<Node, Map<Node, Relationship>> nodeRelations){
        // initializing the value of the list of nodes.
        this.involved_nodes = involved_nodes;
        this.nodeRelations = nodeRelations;
        this.resultantSet = new HashMap<>();

        degreeCentrality = new ArrayList<>();
        this.community_counter = 0;

        // Call to method to apply weakly connected components on graph.
        connectedComponents();
    }

    /**
     * Method that initialized a Boolean Map to false. We need a Boolean map to keep track which nodes have
     * been visited during the community detection procedure.
     * @return A treemap that has as key a Node's ID and value Boolean value indicating if the node
     * has been visited already in the community.
     */
    private HashMap<Node, Boolean> initBooleanMap() {
        // Initializing the Map
        HashMap<Node, Boolean> mapUsed = new HashMap<>();

        // Loop through all the nodes involved in graph to add in Map.
        for (Record node : involved_nodes) {
            // Index Node's data.
            Value rec = node.get("n");

            // Using node's ID to give value false-> NOT visited yet.
            Node key = rec.asNode();
            mapUsed.put(key, false);
        }
        // Return the Map to apply the Algorithm
        return mapUsed;
    }

    /**
     * The start of the Weakly Connected Components algorithm. Initialized the Boolean Map.
     * Loops across all the nodes that are involved in the graph and identifies communities based
     * on their relations and if their relations have already been visited or not.
     */
    void connectedComponents()
    {
        // Mark all the vertices as not visited
        HashMap<Node, Boolean> connected_by_visit = initBooleanMap();

        // ID of the community counter.
        int communityUID = 0;
        // Loop through all the nodes involved.
        for (Record node : involved_nodes) {
            communityUID++;

            // Index the specific node record
            Value rec = node.get("n");
            // Get the nodes ID
            Node key = rec.asNode();

            /**
             * Initialize the beginning state of each Set of nodes for each new community trying to be detected.
             */
            Set<Node> nodes_in_community = new HashSet<>();
            this.resultantSet.put(communityUID, nodes_in_community);

            // Check if the node has been already visited or not to find the community.
            if (!connected_by_visit.get(key)) {
                // Define we are applying a Community detection result.
                community_counter++;
                community_printer = new StringBuilder(community_counter + " - Community Detection===================================================== \n");
                // If not visited, then the community size is 1 (for now)
                clusterSize = 1;
                // Print statements to have a clearer result for the user.
                community_printer.append("Community of Relations ");

                // Call to the recursive function to identify all nodes in community.
                FindCommunity(key, connected_by_visit, communityUID);

                // Print statements to have a clearer result for the user.
                community_printer.append("\n");
                // Only if a cluster is a community and not a signle node, print results to preview
                if (clusterSize > 1) {
                    community_printer.append("Cluster Size is : " + clusterSize);
                    // Print the StringBuilder.
                    System.out.println(community_printer);

                    // Call the centrality algorithm to calculate influence of each node.
                    new DegreeCentrality(degreeCentrality, nodeRelations);
                }
                else {
                    // Removing clusters of smaller size than 1 from the resultant dataset.
                    // Not interested in the size because not a community.
                    resultantSet.remove(communityUID);
                }
                degreeCentrality.clear();
            }
        }
    }

    /**
     * Recursive Method that recursively loops through an Arraylist of nodes that are related with the first
     * node (Map that was constructed in constructor.). It goes through the list because a relation exists
     * and so they can be in the same community if a node has not been visited yet.
     * @param node_in_line = Node ID will need to check if it has been visited.
     * @param connected_by_relation = Boolean data structure holding boolean values that represent if a
     *                              Node has been visited or not.
     */
    void FindCommunity(Node node_in_line, HashMap<Node, Boolean> connected_by_relation, int communityUID) {

        /**
         * Getting the community set and adding the new node in the community.
         */
        Set<Node> community_cluster = this.resultantSet.get(communityUID);
        // Because it is a set it will never add the same node twice. It will always work.
        community_cluster.add(node_in_line);
        this.resultantSet.put(communityUID, community_cluster);

        /**
         * Modifying the boolean map that node has been visited, and moving to neighbour node if possible.
         */
        // Updating the node visited status.
        connected_by_relation.put(node_in_line, true);
        // Print statements to have a clearer result for the user.
        community_printer.append(node_in_line.id() + " ");

        // New node in community added in centrality to calculate the number of nodes related.
        degreeCentrality.add(node_in_line);

        // Recursively go over the related nodes to check if they have been visited
        // or if they should be in the same community cluster.
        HashMap<Node, Relationship> communityID = (HashMap<Node, Relationship>) nodeRelations.get(node_in_line);
        Iterator<Node> iterable_nodes = communityID.keySet().iterator();

        for (Iterator<Node> node = iterable_nodes; node.hasNext(); ) {
            Node node_next = node.next();
            // Checking if related node has been visited.
            if (!connected_by_relation.get(node_next)) {
                // If not visited then, node is added to cluster and cluster size is incrementing.
                clusterSize++;
                FindCommunity(node_next, connected_by_relation, communityUID);
            }
        }
    }

    /**
     * Getter method to obtain the Map of degreeCentrality.
     * @return Map with nodes and their relations in the degree centrality.
     */
    public ArrayList<Node> getDegreeCentrality() {
        return degreeCentrality;
    }

    /**
     * Getter method of the final Set of commnbities to be send for Visualisation purposed.
     */
    public Map<Integer, Set<Node>> getFinal_results()
    {
        return this.resultantSet;
    }
}
