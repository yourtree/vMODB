//package dk.ku.di.dms.vms.modb.query.complex.executor;
//
//import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
//import dk.ku.di.dms.vms.modb.query.complex.planner.tree.PlanNode;
//
//public class SequentialQueryExecutor {
//
//    // descriptor of the result. the schema of the returned entries.
//
//    private PlanNode node;
//
//    public SequentialQueryExecutor(final PlanNode node) {
//        this.node = node;
//    }
//
//    /**
//     * While there are remaining tasks, continue in a loop
//     * to schedule the tasks when their dependencies have been
//     * fulfilled
//     */
//    @Override
//    public IOperatorResult get() {
//
//        IOperatorResult result = null;
//
//        // while we still have a node to schedule
//        while(true){
//
//            // with an executor it would be like that
//            // Future<OperatorResult> futureResult = this.executor.submit( () -> tail.supplier.get() );
//
//            result = node.supplier.get();
//
//            node = node.father;
//
//            if(node == null) break;
//
//            if(node.consumer != null) {
//                node.consumer.accept(result);
//            }
//        }
//
//        return result;
//
//    }
//
//}