/**
 * Libraries && Packages importing in the class to make the build and compile of the code
 * be successful.
 * Importing neo4j driver packages and Collection data structures packages.
 */


import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.*;


/**
 * Skeleton of code has been taken from neo4j guidelines for Java drivers
 * Large part of connecting code has been taken from neo4j Hello World Example
 * Code structure and documentation can be found: https://neo4j.com/docs/java-manual/4.2/get-started/
 *
 * AutoCloseable is an interface to make sure that when disconnected the files
 * and resources have been released!
 * Connector is a java class that will establish a simple connection to the neo4j
 * graph database and apply the pipeline query.
 */
public class Connector implements AutoCloseable {
    // Driver will be our neo4j java driver object that connects java script to neo4j.
    private final Driver driver;
    // Name of the database i will be working with, because their are a lot of them in the server.
    private final String researchDB = "research";

    private static WConnectedComponents wccResult;
    private static LabelPropagation labelResults;
    private static BiasIntroduced biasResults;
    private static OnlyBias onlybiasResults;
    private static Map<Node, Map<Node, Relationship>> nodeRelations;


    // String that gives instrunctions to the user on process.
    private final static String USER_GUIDE = "" +
            "There are two pipelines implemented, choose the following : \n" +
            "[1] Pipeline -> Weakly Connected Components + Degree Centrality. \n" +
            "[2] Pipeline -> Label Propagation + Degree Centrality. \n" +
            "[3] Pipeline -> Label Propagation + Degree Centrality considering biased nodes first in changing labels. \n" +
            "[4] Pipeline -> Label Propagation + Degree Centrality with ONLY the biased nodes from ML \n" +
            "Type the appropriate number from the options [1, 2, 3, 4]. \n" +
            "Option 3 & 4 work for query that includes nodes of type [Datapoint or Ratio]";
    /**
     * The following are neo4j cypher queries being applied through neo4j connection.
     * The main idea behind the queries is to filter the nodes and relations from the database and keep only the ones
     * that will be used in the algorithm and we are mostly interested in.
     */
    private static final String main_query_any = "" +
            "Match (n) - [r] -> (m) " +
            "Return n,r,m";
    private static final String sub_query_any = "" +
            "Match (n) " +
            "Return n";

    // Specific LABEL
    private static final String main_query_label = "" +
            "Match (n:Company) - [r] -> (m:Company) " +
            "Return n,r,m";
    private static final String sub_query_label = "" +
            "Match (n:Company) " +
            "Return n";

    // Specific RELATION
    private static final String main_query_relation = "" +
            "MATCH (n:Disclosure)-[r:DISCLOSURE_CHARACTERISE]->(m:Areas) \n" +
            "RETURN  n, r, m";
    private static final String sub_query_relation = "" +
            "MATCH (n:Disclosure)-[r:DISCLOSURE_CHARACTERISE]->(m:Areas) \n" +
            "Return n AS n " +
            "UNION " + // Union allows to merger two results without producing duplicates.
            "MATCH (n:Disclosure)-[r:DISCLOSURE_CHARACTERISE]->(m:Areas) \n" +
            "Return m AS n ";


    // Specific LABEL & RELATION <- Find this filtering query.
    private static final String main_query_both = "" +
            "Match (n:Datapoint) - [r:RATES] -> (m:Datapoint) " +
            "Return n, r, m";
    private static final String sub_query_both = "" +
            "Match (n:Datapoint) - [r:RATES] -> (m) " +
            "Return n " +
            "UNION " +
            "Match (n) - [r:RATES] -> (m) " +
            "Return m AS n";

    // ALL Nodes that are suspected as suspicious from machine learning techniques.
    private static final String BIAS_NODES = "" +
            "MATCH ()-[r:FOUND_ML_OUTLIER]->(n) " +
            "Return n " +
            "UNION " +
            "MATCH ()-[r:FOUND_MS_AD_OUTLIER]->(n) " +
            "Return n";


    /**
     * This is the Connector construction that will give driver the appropriate parameters and establish
     * the connection.
     *
     * @param uri      = URL of the location of the database.
     * @param user     = The name of the database.
     * @param password = Password of the database.
     */
    public Connector(String uri, String user, String password) {
        // Initializing the driver object to connect.
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    /**
     * Method that AutoCloseable implements. This is when the connection fails and closes, it makes sure
     * the driver is closed and the resources are released.
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        driver.close();
    }

    /**
     * Method that executes a query and returns the collection of nodes and relations of the query.
     *
     * @param query = The query we want to apply.
     * @return The type we want to return which is a List of nodes and edges.
     */
    public List<Record> executeSimpleQuery(String query) {
        try (Session session = driver.session(SessionConfig.forDatabase(researchDB))) {
            // Executing the query.
            List<Record> result = session.readTransaction(
                    tx -> tx.run(query).list());

            return result;
        }
    }

    /**
     * Main method, this is the controller and the flow of the program that will follow. Methods are
     * controlled and called from here.
     *
     * @param args = Accepts commands from terminal or cdm
     * @throws Exception
     */
    public static void main(String... args) throws Exception {
        try (Connector graphDB = new Connector("???", "???", "???")) {
            // Get the number of nodes in the graph.
            List<Record> number_nodes = graphDB.executeSimpleQuery(sub_query_relation);
            //printSingleNodes(number_nodes);

            // Apply query to all nodes and edges
            List<Record> full_graph = graphDB.executeSimpleQuery(main_query_relation);
            //printAllGraphResults(full_graph);


            // Give the size of graph and its list.
            GraphDB graph_nodes = new GraphDB(number_nodes, full_graph);
            nodeRelations = graph_nodes.getNodeRelations();

            boolean option = false;
            // Assign scanner to keyboard variable
            Scanner keyboard = new Scanner(System.in);
            while (!option) {
                System.out.println(USER_GUIDE);
                int option_chosen = keyboard.nextShort();

                // For all the options except 1.
                Map<Node, Map<Node, Relationship>> correctMap = new HashMap<>();
                correctMap.putAll(nodeRelations);

                if (option_chosen == 1) {
                    wccResult = new WConnectedComponents(number_nodes, nodeRelations);
                    option = true;

                    /**
                     * At this stage pipeline has been applied, now we have the resultant structures and need to only visualise them.
                     * Below code connects on a local enterprise edition neo4j (Local DB) which it manually creates nodes and relations to
                     * represent the knowledge graph constructed from pipeline algorithms.
                     */
                    try (VisualGraph project = new VisualGraph("???", "???", "???", wccResult.getFinal_results(), nodeRelations)) {
                        project.projectNodesVisualLPA();
                    }
                } else if (option_chosen == 2) {
                    labelResults = new LabelPropagation(number_nodes, correctMap);
                    labelResults.find_labelPropagation_communities();
                    // In label propagation we remove nodes slowly from nodeRelations so we need to re-initialize it.
                    nodeRelations = graph_nodes.getNodeRelations();
                    option = true;

                    /**
                     * At this stage pipeline has been applied, now we have the resultant structures and need to only visualise them.
                     * Below code connects on a local enterprise edition neo4j (Local DB) which it manually creates nodes and relations to
                     * represent the knowledge graph constructed from pipeline algorithms.
                     */
                    try (VisualGraph project = new VisualGraph("???", "???", "???", labelResults.getFinal_results(), nodeRelations)) {
                        project.projectNodesVisualLPA();
                    }
                } else if (option_chosen == 3) {
                    List<Record> biased_nodes = graphDB.executeSimpleQuery(BIAS_NODES);
                    // Set of nodes that will hold all the nodes that are biased.
                    Set<Node> bias_node = new HashSet<>();

                    // Converting the records retrieved to nodes, so we can send to Label Propagation class.
                    for (Record rec: biased_nodes) {
                        // Index node's record to create Structure according to their ID.
                        Value values = rec.get("n");

                        // Initializing the Map structure.
                        Node key = values.asNode();
                        bias_node.add(key);
                    }

                    biasResults = new BiasIntroduced(number_nodes, correctMap, bias_node);
                    biasResults.find_labelPropagation_communities();
                    // In label propagation we remove nodes slowly from nodeRelations so we need to re-initialize it.
                    nodeRelations = graph_nodes.getNodeRelations();
                    option = true;

                    /**
                     * At this stage pipeline has been applied, now we have the resultant structures and need to only visualise them.
                     * Below code connects on a local enterprise edition neo4j (Local DB) which it manually creates nodes and relations to
                     * represent the knowledge graph constructed from pipeline algorithms.
                     */
                    try (VisualGraph project = new VisualGraph("???", "???", "???", biasResults.getFinal_results(), nodeRelations)) {
                        project.projectNodesVisualLPA();
                    }
                } else if (option_chosen == 4) {
                    List<Record> biased_nodes = graphDB.executeSimpleQuery(BIAS_NODES);
                    // Set of nodes that will hold all the nodes that are biased.
                    Set<Node> bias_node = new HashSet<>();

                    // Converting the records retrieved to nodes, so we can send to Label Propagation class.
                    for (Record rec: biased_nodes) {
                        // Index node's record to create Structure according to their ID.
                        Value values = rec.get("n");

                        // Initializing the Map structure.
                        Node key = values.asNode();
                        bias_node.add(key);
                    }

                    onlybiasResults = new OnlyBias(number_nodes, correctMap, bias_node);
                    onlybiasResults.find_labelPropagation_communities();
                    // In label propagation we remove nodes slowly from nodeRelations so we need to re-initialize it.
                    nodeRelations = graph_nodes.getNodeRelations();
                    option = true;

                    /**
                     * At this stage pipeline has been applied, now we have the resultant structures and need to only visualise them.
                     * Below code connects on a local enterprise edition neo4j (Local DB) which it manually creates nodes and relations to
                     * represent the knowledge graph constructed from pipeline algorithms.
                     */
                    try (VisualGraph project = new VisualGraph("???", "???", "???", onlybiasResults.getFinal_results(), nodeRelations)) {
                        project.projectNodesVisualLPA();
                    }
                }

            }
        }


    }
}
