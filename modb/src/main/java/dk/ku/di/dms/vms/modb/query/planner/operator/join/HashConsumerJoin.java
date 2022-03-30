package dk.ku.di.dms.vms.modb.query.planner.operator.join;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.store.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.store.common.IKey;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * A hash join where its dependencies are not fulfilled from the start
 * For example, when it is necessary to perform a sequential scan
 * before fulfilling the join
 * TODO generalize for left, right, outer join
 */

public class HashConsumerJoin extends AbstractJoin
        implements BiConsumer<CompletableFuture<RowOperatorResult>,CompletableFuture<RowOperatorResult>> {

    private CompletableFuture<RowOperatorResult> operatorResultFutureLeft;
    private CompletableFuture<RowOperatorResult> operatorResultFutureRight;

    public HashConsumerJoin(int identifier, AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(identifier, innerIndex, outerIndex);
    }

    @Override
    public JoinOperatorTypeEnum getType() {
        return JoinOperatorTypeEnum.HASH;
    }

    @Override
    public void accept(CompletableFuture<RowOperatorResult> operatorResultFutureLeft, CompletableFuture<RowOperatorResult> operatorResultFutureRight) {
        this.operatorResultFutureLeft = operatorResultFutureLeft;
        this.operatorResultFutureRight = operatorResultFutureRight;
    }

    @Override
    public RowOperatorResult get() {

        // CompletableFuture.allOf( operatorResultFutureLeft, operatorResultFutureRight ).
        try {
            RowOperatorResult left = operatorResultFutureLeft.get();
            RowOperatorResult right = operatorResultFutureRight.get();

            // TODO continue

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return null;

    }
}
