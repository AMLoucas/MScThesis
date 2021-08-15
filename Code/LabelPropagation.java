/**
 * Libraries && Packages importing in the class to make the build and compile of the code
 * be successful.
 * Importing neo4j driver packages and Collection data structures packages.
 */
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import java.util.*;

/**
 * CODE in this class is original and has been created from scratch. It is my own contribution in the project.
 * The whole class and mechanism are based on the below pseudocode.
 *
 * PSEUDO CODE
 * 1. Initialize the labels at all nodes in the network. For a given node x, Cx (0) = x.
 * 2. Set t = 1.
 * 3. Arrange the nodes in the network in a random order and set it to X.
 * 4. For each x ∈ X chosen in that specific order, let Cx(t) = f(Cxi1(t), ...,Cxim(t),Cxi(m+1) (t − 1), ...,Cxik (t − 1)).
 *      Here returns the label occurring with the highest frequency among neighbours. Select a label at random if there
 *      are multiple highest frequency labels.
 * 5. If every node has a label that the maximum number of their neighbours have, then stop the algorithm. Else, set t = t + 1 and go to (3).
 */ *


public class LabelPropagation {

    /**
     * Class variables that will be used within the Label Propagation algorithm.
     * nodeRelations = Map that holds the network being used in algorithm. It holds all the nodes in tha graph and the relationships
     * between the nodes.
     * node_labels = It holds the Node ID and the label of the node at time T.
     * node_labels_past = It holds the Node ID and the label of the node at time (T-1)
     * all_nodes = It is a list that holds all the nodes that are involved in the graph/network.
     * <p>
     * mapBeingModified = It is a Map that over time (T) is being modified and changed to form clusters of communities and and labels change.
     * Single clusters are being removed, duplicate clusters are being removed and etc.
     * labels_changed = It is a boolean value that holds a value that represents if the node_labels equals with node_labels_past
     * it is used for a stopping criteria condition to see if the iteration made label changes.
     * netowrk_keys = List of nodes in a random order used to propagate the labels.
     */
    protected final Map<Node, Map<Node, Relationship>> nodeRelations;
    protected Map<Integer, Integer> node_labels;
    protected Map<Integer, Integer> node_labels_past;
    protected final List<Record> all_nodes;
    protected List<Node> network_keys;
    protected Map<Node, Map<Node, Relationship>> mapBeingModified;
    protected boolean labels_changed;
    protected Map<Integer, Set<Node>> final_results;

    protected final int MAX_ITER = 5;

    /**
     * Constructor of class Label Propagation Community Detector algorithm.
     *
     * @param all_nodes     = All the nodes involved in the graph.
     * @param nodeRelations = All the connected nodes involved in the graph, together with their relationships and
     *                      relationship details.
     */
    public LabelPropagation(List<Record> all_nodes, Map<Node, Map<Node, Relationship>> nodeRelations) {
        this.all_nodes = all_nodes;
        this.nodeRelations = nodeRelations;
        // Cloning the map
        this.mapBeingModified = new HashMap<>(this.nodeRelations);
        // Giving initial unique labels to each Node.
        this.node_labels = initLabels();

        //Call of main detector algorithm to find the communities.
        //find_labelPropagation_communities();

    }


    /**
     * Method that initializes all the initial labels of the nodes. In the beginning as STEP [1] confirms, each node is assigned a
     * different community label.
     *
     * @return A Map that has as Key a node ID and as a value a unique label as a starting phase.
     */
    protected Map<Integer, Integer> initLabels() {
        Map<Integer, Integer> labels = new HashMap<>();

        int i = 1;
        // Loop through all the nodes involved in the knowledge graph and assign a unique label.
        for (Record node : this.all_nodes) {
            Node node_data = node.get("n").asNode();
            int node_id = Integer.parseInt(String.valueOf(node_data.id()));
            // Assigning Key-Value pairs (Node, label)
            labels.put(node_id, i);
            // Incrementing label so every label is unique.
            i = i + 1;
        }
        return labels;
    }

    /**
     * Method in charge and control to undertake STEP [4] of the pseudocode.
     */

    protected void find_labelPropagation_communities() {
        /**
         * Removing all the nodes with no neighbours and relations.
         * They also offer information in structure, however the time-complexity increases a lot.
         */
        removeSingleNodes();
        /**
         * Using DO-WHILE because code will always run at least once and match labels.
         * So we shuffle the keys at least one, we need to shuffle keys in every iteration.
         */
        int counter = 0;
        boolean max_nei = false;
        do {
            // STEP [3] of the pseudocode is to shuffle the keys and take them to random order.
            this.network_keys = shuffle_nodes();


            /**
             * Find communities of labels.
             */
            // Save the version before altering
            saveVersion();


            // Go through nodes to make changes.
            for (Node node : this.network_keys) {
                detector(node);
            }

            /**
             * Check if any labels changed.
             */
            this.labels_changed = match_versions();
            //System.out.println("Has labels changed? " + this.labels_changed);

            // Check if every node has a label that its maximum neighbour has. (If all YES -> Stop iterating).
            /**
             * Checking the maximum neighbours have same label.
             * It should satisfy for all the nodes!!!
             */
            max_nei = checkNeighboursLabel();
            System.out.println("Does node have labels of maximum neighbour? : " + max_nei);


            counter = counter + 1;

            // Default maxIteration in neo4j manual is 10. [need some working here] && (counter < MAX_ITER)
        } while (labels_changed && !max_nei && MAX_ITER > counter);

        printCommunities();
    }

    protected boolean checkNeighboursLabel() {
        int maximum_label = 0;
        int label_occurence = Integer.MIN_VALUE;
        Map<Integer, Integer> label_counter_neighbourhood = new HashMap<>();
        for (Node master_node : this.mapBeingModified.keySet()) {
            Integer master_label = this.node_labels.get(Integer.parseInt(String.valueOf(master_node.id())));
            Map<Node, Relationship> neighbourhood = this.mapBeingModified.get(master_node);
            for (Node sub_node: neighbourhood.keySet()) {
                Integer sub_label = this.node_labels.get(Integer.parseInt(String.valueOf(sub_node.id())));
                if (!label_counter_neighbourhood.containsKey(sub_label)) {
                    label_counter_neighbourhood.put(sub_label, 1);
                }
                else {
                    label_counter_neighbourhood.put(sub_label, label_counter_neighbourhood.get(sub_label));
                    if (label_occurence <= label_counter_neighbourhood.get(sub_label)) {
                        maximum_label = sub_label;
                        label_occurence = label_counter_neighbourhood.get(sub_label);
                    }

                }
            }

            if (!master_label.equals(maximum_label))
                return false;

        }
        return true;
    }

    /**
     * Method that removes all the nodes from the network that have no neighbours.
     * Neighbours = In this case a neighbour is when a node has a relation to another neighbour.
     */
    protected void removeSingleNodes() {

        for (Record node : this.all_nodes) {
            Node node_data = node.get("n").asNode();
            Map<Node, Relationship> node_relations = this.mapBeingModified.get(node_data);

            // If node does not have any neighbours, remove the record from network.
            if (node_relations.size() < 1) {
                this.mapBeingModified.remove(node_data);
            }
        }
    }

    /**
     * Method the undertakes STEP [3] of the algorithm. It shuffles the nodes in random order before changing the labels depending on
     * neighborhood.
     *
     * @return A List of the nodes in a random order.
     */
    // Although a HashMap is ordered by has codes and not on the values, it is already in Random order.
    // We can still use shuffle() method from package collection to re-randomize the order.
    // Idea was taken from : https://stackoverflow.com/questions/6017338/how-do-you-shuffle-elements-in-a-map
    protected List<Node> shuffle_nodes() {

        // Getting the Key values of the network.
        List<Node> shuffled = new ArrayList<>(this.mapBeingModified.keySet());
        // Shuffling the keys in random order
        Collections.shuffle(shuffled);
        // Returning the shuffled List of keys.
        return shuffled;
    }

    /**
     * Method that saves the past version so comparison matching can occur in the latter stage.
     */
    protected void saveVersion() {
        this.node_labels_past = new HashMap<>(this.node_labels);
    }

    /**
     * Method changes the labels of nodes depending on their neighbours to create communities according
     * to label propagation mechanisms.
     *
     * @param node = A Node in the graph that has relations and a label.
     */
    protected void detector(Node node) {
        // Create a Map the will hold a counter of the most common label of the neighbours.
        HashMap<Integer, Integer> label_counter = new HashMap<>();

        // Getting the Subnetwork to check the labels.
        Map<Node, Relationship> sub_network = this.mapBeingModified.get(node);

        // Getting all the labels in of neighbours and counting their occurrences.
        for (Node iterator : sub_network.keySet()) {
            Integer label = this.node_labels.get(Integer.parseInt(String.valueOf(iterator.id())));

            // If already found before, increment the occurrence
            if (label_counter.containsKey(label))
                label_counter.put(label, (label_counter.get(label) + 1));
                // If not already found before, initialise occurrence to 1.
            else
                label_counter.put(label, 1);
        }

        // Getting maximum occurred label to change label
        this.node_labels.put(Integer.parseInt(String.valueOf(node.id())), label_shuffler(label_counter));
    }

    /**
     * When a node has two or more max labels coming frm neighbours, the label it takes is random and not fixed.
     * We shuffle the labels and take the first one after the shuffling.
     *
     * @param labels = They are the labels that are considered to be taken.
     * @return its an integer value which represents the label.
     */
    protected int label_shuffler(HashMap<Integer, Integer> labels) {
        // Getting the Key values of the network.
        List<Integer> shuffled = new ArrayList<>(labels.keySet());
        // Shuffling the keys in random order
        Collections.shuffle(shuffled);

        // Variables capturing the most featured label.
        int max_counter = Integer.MIN_VALUE;
        int final_label = 0;

        for (Integer key : shuffled) {
            if (labels.get(key) > max_counter) {
                max_counter = labels.get(key);
                final_label = key;
            }
        }

        return final_label;
    }


    /**
     * This checks if the new N_iter network has the same labels as previous (N_iter - 1) network.
     * If nothing has changed then that means that stopping criteria has been met and should not
     * be applied again.
     *
     * @return boolean value that indicates if the labels have changed or not.
     */
    protected boolean match_versions() {
        for (Integer node : this.node_labels.keySet()) {
            if (!this.node_labels.get(node).equals(this.node_labels_past.get(node)))
                return true;
        }
        return false;
    }

    /**
     * Setter for the main Map that holds data on label propagation.
     */
    protected void setFinal_results(Map<Node, Map<Node, Relationship>> printableResults) {
        this.final_results = communityConstructor(printableResults);
    }

    /**
     * Getter method for the main Map that holds final results.
     */
    protected Map<Integer, Set<Node>> getFinal_results()
    {
        return this.final_results;
    }
    /**
     * Method that prints the results in Java UI. It prints the main node ID with its label together with the node's ID that is related and is in the same community.
     * MAIN-PURPOSE: Print to test and check if results are valid!
     */
    protected void printCommunities() {

        Map<Node, Map<Node, Relationship>> printableResults = removeDuplicates();
        setFinal_results(printableResults);

        int counter = 1;
        for (Integer label : final_results.keySet()) {
            Set<Node> node_in_community = final_results.get(label);
            if (node_in_community.size() > 1) {
                System.out.println("[" + counter + "] Community Label is: " + label);
                System.out.println("Size of community is :" + node_in_community.size());
                counter++;

                // Create an ArrayList to get send to degree centrality.
                Set<Node> nodes_degree = new HashSet<>();

                for (Node unique_node : node_in_community) {
                    System.out.print("Node: " + Integer.parseInt(String.valueOf(unique_node.id())) + "  ");
                    nodes_degree.add(unique_node);

                }
                System.out.println();

                // Create a Map with the correct network structure.
                Map<Node, Map<Node, Relationship>> correct_network = new HashMap<>();
                for (Node node_one : node_in_community) {
                    // Adding the key with an empty map of neighbours.
                    Map<Node, Relationship> neighbour = new HashMap<>();
                    correct_network.put(node_one, neighbour);
                    // Getting all the neighbours from the full graph.
                    if (printableResults.containsKey(node_one)) {
                        Map<Node, Relationship> node_one_neighbours = printableResults.get(node_one);
                        for (Node node_two : node_one_neighbours.keySet()) {
                            if (node_in_community.contains(node_two)) {
                                neighbour.put(node_two, node_one_neighbours.get(node_two));
                                // Keep updating neighbourhood. Will overwrite because its a map
                                correct_network.put(node_one, neighbour);

                            }
                        }
                    }
                }
                ArrayList<Node> all_nodes_unique = new ArrayList<>();
                all_nodes_unique.addAll(nodes_degree);
                new DegreeCentrality(all_nodes_unique, correct_network);
            }

        }
    }

    /**
     * Method that removes the duplicates from the resultant set.
     * Because our graph is unidirectional in Java is implemented as bi-directional this means relationships are duplicate facing opposite sides. For this reason when we
     * identify A -> B we automatically identify B -> A however, this the same information being shared. We want to remove this duplicate information so we have less Java UI
     * results and for the visuals to be more clear.
     */
    protected Map<Node, Map<Node, Relationship>> removeDuplicates() {
        Map<Node, Map<Node, Relationship>> nonDuplicates = new HashMap<>();

        // Keeping all communities of size larger than 1 ( size 2 or more )
        for (Node node : nodeRelations.keySet()) {
            Map<Node, Relationship> neighbours = nodeRelations.get(node);
            if (neighbours.size() > 0) {
                nonDuplicates.put(node, neighbours);
            }
        }

        // Remove all neighbours from the large communities.
        for (Node node : nonDuplicates.keySet()) {
            nodeRelations.remove(node);
            Map<Node, Relationship> neighbours = nonDuplicates.get(node);
            for (Node neighbour : neighbours.keySet()) {
                nodeRelations.remove(neighbour);
            }
        }

        /**
         * In the nodeRelations Map now, we only have the communities of size 2 in a duplicate fashion.
         * FOR EXAMPLE => (a, b), (b, a), (c, d), (d, c), ....
         * GOAL: is to remove the duplicates and keep only: (a, b), (c, d), ...
         * Because of the duplication we know there is an even number of communities in the Map left, so now
         * we need to keep only the half of them from the set. So we will iterate through the map N times, (N = half of the Map size)
         * each time before we add a new community we will check if the value has an overlap with the keys and vice vera.
         *
         * By following this tactic we ensure we will have distinct communities with no overlap.
         */

        for (Node node : nodeRelations.keySet()) {
            // Check if the node is contained in the Map
            if (!nonDuplicates.containsKey(node)) {
                // If its not contained, check if the value is contained.
                Map<Node, Relationship> neighbours = nodeRelations.get(node);
                for (Node value_node : neighbours.keySet()) {
                    // Check if the neighbour of community is in the Mao
                    if (!nonDuplicates.containsKey(value_node))
                        // If the key and the value node is not in the Map, add the community of size 2.
                        nonDuplicates.put(node, neighbours);
                }
            }
        }

        // Return a Map with no overlap and duplicates.
        return nonDuplicates;
    }


    /**
     * Purpose of this function is to translate the cluster of triangles detected to community clusters.
     * Up to now we identified clusters and labelled them, however we might have multiple clusters with the same community label
     * but this means that all the clusters are connected and are in the same community.
     *
     * For this reason we will re-create a Map Data Structure that will hold the community clusters as whole.
     */
    protected Map<Integer, Set<Node>> communityConstructor(Map<Node, Map<Node, Relationship>> clusters) {
        Map<Integer, Set<Node>> communityClusters = new HashMap<>();

        // Using a Set so we automatically have no duplicates.
        Set<Node> all_labelled_nodes = new HashSet<>();

        // Putting the nodes inn the list.
        for (Node node: clusters.keySet()) {
            // Add key node in the set.
            all_labelled_nodes.add(node);
            Map<Node, Relationship> neighbours = clusters.get(node);
            // Add all the neighbours of the key node into the set.
            all_labelled_nodes.addAll(neighbours.keySet());

        }

        // Going through the clusters to get labels and structure to communities.
        for (Node key_node : all_labelled_nodes) {

            // getting the label id th
            int label = node_labels.get(Integer.parseInt(String.valueOf(key_node.id())));
            if (!communityClusters.containsKey(label)) {
                Set<Node> node = new HashSet<>();
                node.add(key_node);
                communityClusters.put(label, node);
            }
            else {
                Set<Node> node = communityClusters.get(label);
                node.add(key_node);
                communityClusters.put(label, node);
            }
        }

        return communityClusters;
    }

}