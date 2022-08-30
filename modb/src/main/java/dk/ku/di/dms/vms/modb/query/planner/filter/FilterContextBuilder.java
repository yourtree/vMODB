package dk.ku.di.dms.vms.modb.query.planner.filter;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.types.TypedBiPredicate;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class FilterContextBuilder {

    public Predicate<Object> predicates;

    public static Predicate<?> predicateNull = Objects::isNull;

    public static Predicate<?> predicateNotNull = Objects::nonNull;

    // public static TypedFunction<int> intGtThanZeroFunction = t -> t > 0;

    public static TypedBiPredicate<int> intGtPredicate = ((t1, t2) -> t1 > t2);

    public static TypedBiPredicate<int> intGtOrEqPredicate = ((t1, t2) -> t1 >= t2);

    public static TypedBiPredicate<int> intLtPredicate = ((t1, t2) -> t1 < t2);

    public static TypedBiPredicate<int> intLtOrEqPredicate = ((t1, t2) -> t1 <= t2);

    public static TypedBiPredicate<int> intEqPredicate = ((t1, t2) -> t1 == t2);

    public static TypedBiPredicate<int> intNotEqPredicate = ((t1, t2) -> t1 != t2);

    private static TypedBiPredicate<int> getIntValuePredicate(ExpressionTypeEnum expressionType){
        switch (expressionType){
            case GREATER_THAN : return intGtPredicate;
            case GREATER_THAN_OR_EQUAL: return intGtOrEqPredicate;
            case LESS_THAN: return intLtPredicate;
            case LESS_THAN_OR_EQUAL: return intLtOrEqPredicate;
            case EQUALS: return intEqPredicate;
            case NOT_EQUALS: return intNotEqPredicate;
            default: throw new IllegalStateException("FAIL");
        }
    }

    private static boolean isNullPredicate(ExpressionTypeEnum expressionType){
        return expressionType == ExpressionTypeEnum.IS_NOT_NULL || expressionType == ExpressionTypeEnum.IS_NULL;
    }

    private static Predicate<?> getNullPredicate(ExpressionTypeEnum expressionType){
        if(expressionType == ExpressionTypeEnum.IS_NULL) return predicateNull;
        return predicateNotNull;
    }

    public static FilterContext build(List<WherePredicate> wherePredicates){

        int size = wherePredicates.size();
        FilterType[] filterTypes = new FilterType[size];

        TypedBiPredicate<?>[] biPredicates = new TypedBiPredicate<?>[size];

        Predicate<?>[] predicates = new Predicate[size];

        for(WherePredicate wherePredicate : wherePredicates){

            DataType dataType = wherePredicate.columnReference.dataType;
            ExpressionTypeEnum expressionType = wherePredicate.expression;

            switch(dataType){

                case INT: {
                    if(isNullPredicate(expressionType)){
                        predicates[0] = getNullPredicate(expressionType);
                    } else {
                        TypedBiPredicate<int> predicate = getIntValuePredicate(expressionType);
                        biPredicates[0] = predicate;
                    }
                }
                case CHAR: {
//                    filter = getFilter( wherePredicate.expression, Character::compareTo );
//                    break;
                }
                case LONG: {
//                    filter = getFilter( wherePredicate.expression, Long::compareTo );
//                    break;
                }
                case DOUBLE: {
//                    filter = getFilter( wherePredicate.expression, Double::compareTo);
//                    break;
                }
                case DATE: {
//                    filter = getFilter( wherePredicate.expression, Double::compareTo);
//                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + dataType);
            }


        }

        return null;
    }

}
