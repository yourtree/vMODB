package dk.ku.di.dms.vms.database.query.planner.node.filter.expr;

import java.io.Serializable;
import java.util.function.Predicate;

public interface IFilterPredicate<V extends Serializable> extends Predicate<V> {

}
