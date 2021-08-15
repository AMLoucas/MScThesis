import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BiasIntroduced  extends  LabelPropagation{

    Set<Node> bias_node;

    public BiasIntroduced(List<Record> all_nodes, Map<Node, Map<Node, Relationship>> nodeRelations, Set<Node> bias_node) {
        super(all_nodes, nodeRelations);

        this.bias_node =  find_biased_included(bias_node);
    }

    /**
     * Function that finds the biased nodes.
     * Biased nodes are nodes that were detected suspicious from the machine learning algorithms and are also contained in
     * the candidate nodes of the knowledge graph being constructed.
     * @param all_biased are all the nodes detected as suspicious by ML procedures.
     * @return Set of nodes that were detected as suspicious by ML procedures.
     */
    private Set<Node> find_biased_included(Set<Node> all_biased) {
        for (Record node: all_nodes) {
            Node node_data = node.get("n").asNode();
            if (!all_biased.contains(node_data))
                all_biased.remove(node_data);
        }
        return all_biased;
    }

    /**
     * Method in charge and control to undertake STEP [4] of the pseudocode.
     * However we first go through the biased nodes and later through the other nodes, not a random shuffle.
     */
    @Override
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

            try {
                // Go thourgh biased nodes first
                for (Node node : this.bias_node) {
                    detector(node);
                }

                // Remove biased nodes.
                this.network_keys.removeIf(node -> bias_node.contains(node));
            } catch (Exception e) {
                //do nothing.
            }

            // Go thourgh the other nodes.
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

    /**
     * Method that removes all the nodes from the network that have no neighbours.
     * Neighbours = In this case a neighbour is when a node has a relation to another neighbour.
     * We are also removing the nodes from the biased set.
     */
    @Override
    protected void removeSingleNodes() {
        for (Record node : this.all_nodes) {
            Node node_data = node.get("n").asNode();
            Map<Node, Relationship> node_relations = this.mapBeingModified.get(node_data);

            // If node does not have any neighbours, remove the record from network.
            if (node_relations.size() < 1) {
                this.mapBeingModified.remove(node_data);
                // remove from biased nodes as well.
                try{
                    this.bias_node.remove(node_data);
                }
                catch (Exception e){
                    // Do nothing.
                }
            }
        }
    }
}
