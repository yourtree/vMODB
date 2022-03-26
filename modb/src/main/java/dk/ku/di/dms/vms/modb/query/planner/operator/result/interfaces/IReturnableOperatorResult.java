package dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces;

import java.util.function.Consumer;

/**
 * Interface to embrace the notion of a result provided by an operator as a result of a computation
 */
public interface IReturnableOperatorResult<T> extends Consumer<T>, IOperatorResult {}