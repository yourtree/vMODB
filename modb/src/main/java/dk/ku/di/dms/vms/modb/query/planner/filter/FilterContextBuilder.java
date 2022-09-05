package dk.ku.di.dms.vms.modb.query.planner.filter;

import dk.ku.di.dms.vms.modb.api.type.DataType;
import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.types.TypedBiPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class FilterContextBuilder {

    public static Predicate<Object> predicateNull = Objects::isNull;
    public static Predicate<Object> predicateNotNull = Objects::nonNull;

    public static final TypedBiPredicate<int> intGtPredicate = ((t1, t2) -> t1 > t2);
    public static final TypedBiPredicate<int> intGtOrEqPredicate = ((t1, t2) -> t1 >= t2);
    public static final TypedBiPredicate<int> intLtPredicate = ((t1, t2) -> t1 < t2);
    public static final TypedBiPredicate<int> intLtOrEqPredicate = ((t1, t2) -> t1 <= t2);
    public static final TypedBiPredicate<int> intEqPredicate = ((t1, t2) -> t1 == t2);
    public static final TypedBiPredicate<int> intNotEqPredicate = ((t1, t2) -> t1 != t2);

    private static TypedBiPredicate<int> getIntValuePredicate(ExpressionTypeEnum expressionType){
        return switch (expressionType) {
            case EQUALS -> intEqPredicate;
            case GREATER_THAN -> intGtPredicate;
            case GREATER_THAN_OR_EQUAL -> intGtOrEqPredicate;
            case LESS_THAN -> intLtPredicate;
            case LESS_THAN_OR_EQUAL -> intLtOrEqPredicate;
            case NOT_EQUALS -> intNotEqPredicate;
            default -> throw new IllegalStateException("FAIL");
        };
    }

    public static final TypedBiPredicate<float> floatGtPredicate = ((t1, t2) -> t1 > t2);
    public static final TypedBiPredicate<float> floatGtOrEqPredicate = ((t1, t2) -> t1 >= t2);
    public static final TypedBiPredicate<float> floatLtPredicate = ((t1, t2) -> t1 < t2);
    public static final TypedBiPredicate<float> floatLtOrEqPredicate = ((t1, t2) -> t1 <= t2);
    public static final TypedBiPredicate<float> floatEqPredicate = ((t1, t2) -> t1 == t2);
    public static final TypedBiPredicate<float> floatNotEqPredicate = ((t1, t2) -> t1 != t2);

    private static TypedBiPredicate<float> getFloatValuePredicate(ExpressionTypeEnum expressionType){
        return switch (expressionType) {
            case EQUALS -> floatEqPredicate;
            case GREATER_THAN -> floatGtPredicate;
            case GREATER_THAN_OR_EQUAL -> floatGtOrEqPredicate;
            case LESS_THAN -> floatLtPredicate;
            case LESS_THAN_OR_EQUAL -> floatLtOrEqPredicate;
            case NOT_EQUALS -> floatNotEqPredicate;
            default -> throw new IllegalStateException("FAIL");
        };
    }

    public static final TypedBiPredicate<double> doubleGtPredicate = ((t1, t2) -> t1 > t2);
    public static final TypedBiPredicate<double> doubleGtOrEqPredicate = ((t1, t2) -> t1 >= t2);
    public static final TypedBiPredicate<double> doubleLtPredicate = ((t1, t2) -> t1 < t2);
    public static final TypedBiPredicate<double> doubleLtOrEqPredicate = ((t1, t2) -> t1 <= t2);
    public static final TypedBiPredicate<double> doubleEqPredicate = ((t1, t2) -> t1 == t2);
    public static final TypedBiPredicate<double> doubleNotEqPredicate = ((t1, t2) -> t1 != t2);

    private static TypedBiPredicate<double> getDoubleValuePredicate(ExpressionTypeEnum expressionType){
        return switch (expressionType) {
            case EQUALS -> doubleEqPredicate;
            case GREATER_THAN -> doubleGtPredicate;
            case GREATER_THAN_OR_EQUAL -> doubleGtOrEqPredicate;
            case LESS_THAN -> doubleLtPredicate;
            case LESS_THAN_OR_EQUAL -> doubleLtOrEqPredicate;
            case NOT_EQUALS -> doubleNotEqPredicate;
            default -> throw new IllegalStateException("FAIL");
        };
    }

    public static final TypedBiPredicate<long> longGtPredicate = ((t1, t2) -> t1 > t2);
    public static final TypedBiPredicate<long> longGtOrEqPredicate = ((t1, t2) -> t1 >= t2);
    public static final TypedBiPredicate<long> longLtPredicate = ((t1, t2) -> t1 < t2);
    public static final TypedBiPredicate<long> longLtOrEqPredicate = ((t1, t2) -> t1 <= t2);
    public static final TypedBiPredicate<long> longEqPredicate = ((t1, t2) -> t1 == t2);
    public static final TypedBiPredicate<long> longNotEqPredicate = ((t1, t2) -> t1 != t2);

    private static TypedBiPredicate<long> getLongValuePredicate(ExpressionTypeEnum expressionType){
        return switch (expressionType) {
            case EQUALS -> longEqPredicate;
            case GREATER_THAN -> longGtPredicate;
            case GREATER_THAN_OR_EQUAL -> longGtOrEqPredicate;
            case LESS_THAN -> longLtPredicate;
            case LESS_THAN_OR_EQUAL -> longLtOrEqPredicate;
            case NOT_EQUALS -> longNotEqPredicate;
            default -> throw new IllegalStateException("FAIL");
        };
    }

    public static final TypedBiPredicate<boolean> boolEqPredicate = ((t1, t2) -> t1 == t2);
    public static final TypedBiPredicate<boolean> boolNotEqPredicate = ((t1, t2) -> t1 != t2);

    private static TypedBiPredicate<boolean> getBooleanValuePredicate(ExpressionTypeEnum expressionType){
        return switch (expressionType) {
            case EQUALS -> boolEqPredicate;
            case NOT_EQUALS -> boolNotEqPredicate;
            default -> throw new IllegalStateException("FAIL");
        };
    }

    public static final TypedBiPredicate<char[]> charEqPredicate = ((t1, t2) -> String.valueOf(t1).contentEquals(String.valueOf(t2)));
    public static final TypedBiPredicate<char[]> charNotEqPredicate = ((t1, t2) -> !String.valueOf(t1).contentEquals(String.valueOf(t2)));

    private static TypedBiPredicate<char[]> getCharArrayValuePredicate(ExpressionTypeEnum expressionType){
        return switch (expressionType) {
            case EQUALS -> charEqPredicate;
            case NOT_EQUALS -> charNotEqPredicate;
            default -> throw new IllegalStateException("FAIL");
        };
    }

    private static boolean isNullPredicate(ExpressionTypeEnum expressionType){
        return expressionType == ExpressionTypeEnum.IS_NOT_NULL || expressionType == ExpressionTypeEnum.IS_NULL;
    }

    private static Predicate<Object> getNullPredicate(ExpressionTypeEnum expressionType){
        if(expressionType == ExpressionTypeEnum.IS_NULL) return predicateNull;
        return predicateNotNull;
    }

    public static FilterContext build(List<WherePredicate> wherePredicates){

        int size = wherePredicates.size();

        FilterContext filterContext = new FilterContext();
        filterContext.filterColumns = new ArrayList<int>(size);

        filterContext.filterTypes = new ArrayList<>(size);
        // bipredicates usually dominate the workload
        filterContext.biPredicates = new ArrayList<>(size);
        filterContext.biPredicateParams = new ArrayList<>(size);
        filterContext.predicates = new ArrayList<>();

        for(WherePredicate wherePredicate : wherePredicates){

            filterContext.filterColumns.add( wherePredicate.columnReference.columnPosition );
            DataType dataType = wherePredicate.columnReference.dataType;
            ExpressionTypeEnum expressionType = wherePredicate.expression;

            if(isNullPredicate(expressionType)){
                filterContext.predicates.add( getNullPredicate(expressionType) );
                filterContext.filterTypes.add(FilterType.P);
                continue;
            }

            filterContext.filterTypes.add(FilterType.BP);
            filterContext.biPredicateParams.add( wherePredicate.value );

            switch(dataType){

                case INT: {
                    filterContext.biPredicates.add(getIntValuePredicate(expressionType));
                }
                case CHAR: {
                    filterContext.biPredicates.add(getCharArrayValuePredicate(expressionType));
                }
                case LONG: {
                    filterContext.biPredicates.add( getLongValuePredicate(expressionType) );
                }
                case DOUBLE: {
                    filterContext.biPredicates.add( getDoubleValuePredicate(expressionType) );
                }
                case FLOAT: {
                    filterContext.biPredicates.add( getFloatValuePredicate(expressionType) );
                }
                case DATE: {
                    filterContext.biPredicates.add( getLongValuePredicate(expressionType) );
                }
                case BOOL: {
                    filterContext.biPredicates.add( getBooleanValuePredicate(expressionType) );
                }
                default: {
                    throw new IllegalStateException("Unexpected value: " + dataType);
                }
            }

        }

        return filterContext;
    }

}
