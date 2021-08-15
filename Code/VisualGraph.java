/**
 * Importing the neo4j Java driver data structures, to be able to use the Maps of Nodes and Relationships appropriately.
 * Importing Data Structure packages as well to use the operations correctly.
 */
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.Map;
import java.util.Set;


/**
 * Class that will get as an input the network and the goal is to create manually the nodes and edges of the knowledge graph
 * and create a visual representation of the graph in neo4j browser enterprise.
 * -> We have the knowledge graph saved in a Java Data Structure, now we use these Data Structures and neo4j cypher query programming
 * to create the data into a visual representation.
 */
public class VisualGraph implements AutoCloseable {

    private final Driver driver;
    private final Map<Integer, Set<Node>> resultantSet;
    private final Map<Node, Map<Node, Relationship>> nodeRelations;

    /**
     * Constructor of the class.
     * @param host = Host of the localhost being connected.
     * @param username = Username of the server to login
     * @param password = Password of the server to login.
     * @param resultantSet = This is the set from LabelPropagation algorithm to print.
     * @param nodeRelations = This is the network Data Structure that holds all the nodes anf their relations.
     * => An automatic call is being done to method deletePrevious -> To make sure all the history and past knowledge
     *                      graphs projected have been removed so we can view the new projected knowledge graph in isolation.
     */
    public VisualGraph(String host, String username, String password, Map<Integer, Set<Node>> resultantSet, Map<Node, Map<Node, Relationship>> nodeRelations){
        this.driver = GraphDatabase.driver( host, AuthTokens.basic( username, password ) );
        this.resultantSet = resultantSet;
        this.nodeRelations = nodeRelations;
        // Remove all previous data from the neo4j server.
        deletePrevious();
    }

    /**
     * Method that needs to be implemented with the AutoCloseable interface to make sure the server connection is being
     * clsoed and the resources are being released.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        driver.close();
    }

    /**
     * This is the main method of the class.
     * This is where all the iterations across the resultantSet and nodeRelations are being done to find and construct the
     * projected knowledge graph create from the algorithm.
     * From iterating through both datasets we manage to identify and create manually all the nodes and their relations. Use
     * cypher command MERGE to avoid duplicate creation of nodes and relationships.
     */
    public void projectNodesVisualLPA() {
        // Looping through all the communities.
        for (Integer community : resultantSet.keySet()) {
            // Obtaining all Nodes of each community.
            Set<Node> community_nodes = resultantSet.get(community);

            /**
             * Create all nodes in the Community.
             * After we create all nodes, then we will start applying the relationships between the nodes.
             */
            // For each node in community to create.
            for (Node nodeA: community_nodes) {
                // Each node we re-initialize the StringBuilder.
                for (Node nodeB: community_nodes) {
                    // Get a map of all the Nodes A has a relation to.
                    // Map used to see how the nodes in same community is connected.
                    Map<Node, Relationship> network_connections = nodeRelations.get(nodeA);
                    // Checking the relations, if relation and in same community then create in network.
                    if (network_connections.containsKey(nodeB)) {
                        StringBuilder create_node_query = new StringBuilder();
                        try (Session session = driver.session()) {
                            /**
                             * Adding details of nodeA in the query.
                             */
                            String labelA = String.valueOf(nodeA.labels());
                            labelA= labelA.replace("[", "").replace("]","").replace(",","").replace(" ","");
                            create_node_query.append("MERGE (nA:").append(labelA.trim()).append(" {");
                            for (String key : nodeA.keys()) {
                                try {
                                    //Used to throw an exception whenever we are dealing with dates.
                                    nodeA.get(key).asString();
                                    create_node_query.append(key.trim()).append(" : ").append(nodeA.get(key)).append(" ,");
                                }
                                catch (org.neo4j.driver.exceptions.value.Uncoercible e) {
                                    // Injecting manually '' to convert Date to String without changing values.
                                    create_node_query.append(key.trim()).append(" : '").append(nodeA.get(key)).append("' ,");
                                }
                            }
                            create_node_query.deleteCharAt(create_node_query.length()-1);
                            create_node_query.append("}) ");

                            /**
                             * Adding details of nodeB in the query
                             */
                            String labelB = String.valueOf(nodeB.labels());
                            labelB= labelB.replace("[", "").replace("]","").replace(",","").replace(" ","");
                            create_node_query.append("MERGE (nB:").append(labelB.trim()).append(" {");
                            for (String key : nodeB.keys()) {
                                try {
                                    //Used to throw an exception whenever we are dealing with dates.
                                    nodeB.get(key).asString();
                                    create_node_query.append(key.trim()).append(" : ").append(nodeB.get(key)).append(" ,");
                                }
                                catch (org.neo4j.driver.exceptions.value.Uncoercible e) {
                                    // Injecting manually '' to convert Date to String without changing values.
                                    create_node_query.append(key.trim()).append(" : '").append(nodeB.get(key)).append("' ,");
                                }
                            }
                            create_node_query.deleteCharAt(create_node_query.length()-1);
                            create_node_query.append("}) ");

                            /**
                             * Adding relationship in the query
                             */
                            Relationship relation = network_connections.get(nodeB);
                            create_node_query.append("WITH nA, nB MERGE (nA) - [r:").append(relation.type()).append("] - (nB)");


                            // Used print to see the query construction is appropriate.
                            //System.out.println(create_node_query);
                            String greeting = session.writeTransaction(new TransactionWork<String>() {
                                @Override
                                public String execute(Transaction tx) {
                                    Result result = tx.run(String.valueOf(create_node_query));
                                    return "New Node";
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    /**
     * Method that removes/deletes all the data that was previously created in the server before.
     * Result is a fresh new database with nothing in it for us to append the new projected knowledge graph.
     */
    private void deletePrevious() {
        try ( Session session = driver.session() )
        {
            String delete = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    Result result = tx.run( "MATCH (n)\n" +
                                    "DETACH DELETE n");
                    return "DELETED DATA";
                }
            } );
            System.out.println( delete );
        }
    }

}
