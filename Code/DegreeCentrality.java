/**
 * Libraries && Packages importing in the class to make the build and compile of the code
 * be successful.
 * Importing neo4j driver packages and Collection data structures packages.
 */

import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.*;

/**
 * Class that implements a Centrality Algorithm to each cluster that a Community Detection algorithm identifies.
 * The Degree Centrality algorithm is the one that is implemented.
 * It has been created from scratch. For each and every relation a node has, it increments the strength/
 * popularity of a node. So a node with the most ingoing && outgoing relationships, it will be the most
 * popular node of the community.
 */
public class DegreeCentrality {
    /**
     * Map that calculates how strongly connected a node is. The more relationships involved with a node
     * the more connected/popular it is.
     */
    private ArrayList<Node> degreeCentrality;
    private Map<Node, Map<Node, Relationship>> nodeRelations;
    private HashMap<Node, Integer> centralityPower;

    public DegreeCentrality(ArrayList<Node> dergreeCentrality, Map<Node, Map<Node, Relationship>> nodeRelations) {
        this.degreeCentrality = dergreeCentrality;
        this.nodeRelations = nodeRelations;
        this.centralityPower = new HashMap<>();

        calculate_centrality();

    }

    /**
     * Code snippet where the degree centrality is being computed. In the graph construction we save the number of nodes a specific node is related to.
     * The power of degree centrality is equal to the number of nodes the specific node is related to no matter the kind-type-direction of relation.
     */
    private void calculate_centrality() {

        for (Node node : degreeCentrality) {
            // Collecting all nodes related to the a specific node in a community.
            Map connected_nodes = nodeRelations.get(node);
            // Add the node in the map and its centrality power.
            centralityPower.put(node, connected_nodes.size());
        }
        print_centrality_powers();
    }

    /**
     * Method to sort a HashMap depending on the value of the key-value pair.
     * Code has been copied/inspired from the following reference: https://www.geeksforgeeks.org/sorting-a-hashmap-according-to-values/
     */
    private void print_centrality_powers() {
        // Create a list from elements of HashMap
        List<Map.Entry<Node, Integer> > sorted = new LinkedList<>(centralityPower.entrySet());

        // Sort the list using lambda expression
        Collections.sort(sorted,
                (value_one, value_two) -> value_two.getValue().compareTo(value_one.getValue()));

        int count = 0;
        for (Map.Entry<Node, Integer> node : sorted) {
            count++;
            System.out.println("["+count+" Popular node]" + node.getKey().labels() + node.getKey().values() + " " +  " has degree power " + node.getValue() );
            if (count == 5)
                break;
        }
    }

}
