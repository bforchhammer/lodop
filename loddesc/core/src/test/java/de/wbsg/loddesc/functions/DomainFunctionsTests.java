package de.wbsg.loddesc.functions;

import com.hp.hpl.jena.graph.query.Domain;
import de.wbsg.loddesc.util.DomainUtils;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class DomainFunctionsTests {
    @Test
    public void payLevelDomainTest() {
        assertEquals(DomainUtils.getPayLevelDomain("www.example.com"),"example.com");
        assertEquals(DomainUtils.getPayLevelDomain("example.com"),"example.com");
        assertEquals(DomainUtils.getPayLevelDomain("www.example.co.uk"),"example.co.uk");
        assertEquals(DomainUtils.getPayLevelDomain("some.more.subs.under.www.example.co.uk"),"example.co.uk");
    }

    @Test
    public void topLevelDomainTest() {
        assertEquals(DomainUtils.getTopLevelDomain("www.example.com"),"com");
        assertEquals(DomainUtils.getTopLevelDomain("example.com"),"com");
        assertEquals(DomainUtils.getTopLevelDomain("www.example.co.uk"),"co.uk");
        assertEquals(DomainUtils.getTopLevelDomain("some.more.subs.under.www.example.co.uk"),"co.uk");
    }

    @Test
    public void getDomainTest() {
        assertEquals(DomainUtils.getDomain("http://www.example.co.uk/foo/bar"),"www.example.co.uk");
        assertEquals(DomainUtils.getDomain("http://www.example.co.uk/"),"www.example.co.uk");
        assertEquals(DomainUtils.getDomain("http://www.example.co.uk"),"www.example.co.uk");
        assertEquals(DomainUtils.getDomain("http://some.more.subs.under.www.example.co.uk/very/stupid"),"some.more.subs.under.www.example.co.uk");
        assertEquals(DomainUtils.getDomain("http://example.co.uk/foo/bar"),"example.co.uk");
    }
}