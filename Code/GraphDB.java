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
 * Class that implements two mechanisms
 * [1] Create the Graph result structure in an Appropriate Java Data Structure that we are able
 * to control and manipulate.
 */
public class GraphDB {

    private final List<Record> full_graph;
    private static Map<Node, Map<Node, Relationship>> nodeRelations;

    /**
     * Constructor that initializes important values and calls the appropriate methods to run the Algorithm.
     * @param involved_nodes = A list that holds all the nodes that are involved in the graph
     * @param full_graph = A list of records of the Triangle relations in the graph. (NodeA-Relation-NodeB)
     */
    public GraphDB(List<Record> involved_nodes, List<Record> full_graph) {

        // Initializing all resulted filtering.
        this.full_graph = full_graph;

        // Creating a Data Structure that will hold the Community of Relations.
        // Outer Structure is a Map which the key is the Node's ID and the value is an
        // ArrayList of Integers that hold the Nodes ID's that are related.
        nodeRelations = new HashMap<>();
        for (Record node: involved_nodes) {
            // Index node's record to create Structure according to their ID.
            Value rec = node.get("n");

            // Initializing the Map structure.
            Node key = rec.asNode();
            nodeRelations.put(key, new HashMap<>());
        }

        // Call to method to Construct the Graph
        init_Graph();
    }

    /**
     * Method that constructs the Graph for the nodes involved in query and problem.
     * Each node that is related with another node, they have two edges (ingoing, outgoing) added to
     * them. We need two relations because it is an undirected graph.
     */
    public void init_Graph() {
        // Loop through the list of records from query.
        for (Record record : full_graph) {
            // Indexing NodeA to get their ID so we can identify them.
            Value node_one = record.get("n");
            Node key_one = node_one.asNode();

            // Indexing NodeB to get their ID so we can identify them.
            Value node_two = record.get("m");
            Node key_two = node_two.asNode();

            // Indexing Relation value of the edge.
            Value relation_edge = record.get("r");
            Relationship relation  = relation_edge.asRelationship();


            // Adding an edge between the two Node's, we use their ID because its unique to connect them.
            nodeRelations.get(key_one).put(key_two, relation);
            nodeRelations.get(key_two).put(key_one, relation);
        }
    }

    /**
     * Getter method for the full graph. Helps with better control of the application.
     * @return the List of Records of he relational return filtering.
     */
    public List<Record> getFull_graph(){
        return full_graph;
    }

    /**
     * Getter method for the Graph creation with bi-directional edges to allow the weakly connected
     * components algorithm work.
     * @return a Map that has as key a Node ID and a List of Node ID's that have a relation with the others.
     */
    public static Map<Node, Map<Node, Relationship>> getNodeRelations() {
        return nodeRelations;
    }


}
