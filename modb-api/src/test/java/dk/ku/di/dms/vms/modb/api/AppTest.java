package dk.ku.di.dms.vms.modb.api;

import dk.ku.di.dms.vms.modb.api.query.parser.Parser;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AppTest 
{
    @Test
    public void testSelectWithOrderByParsing()
    {
        var parsed = Parser.parse("SELECT i_price FROM item WHERE i_id IN (:itemIds) ORDER BY i_id");
        Assert.assertTrue(!parsed.orderByClause.isEmpty());
    }
}
