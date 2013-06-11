package de.wbsg.loddesc.functions;

import de.wbsg.loddesc.util.HashTrie;
import de.wbsg.loddesc.util.Trie;
import de.wbsg.loddesc.util.VocabularyUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class VocabFunctionsTest {

    @Test
    public void trieTest() {
        Trie<Boolean> t = new HashTrie<Boolean>();
        
        t.put("a",true);
        t.put("b",true);
        t.put("aab",true);
        t.put("aabb",true);
        assertNull(t.getLongestMatch("c"));
        
        assertNotNull("b",t.getLongestMatch("b").getKey());
        assertEquals("a",t.getLongestMatch("abbbb").getKey());
        assertEquals("aab",t.getLongestMatch("aabaaa").getKey());
        assertEquals("aabb",t.getLongestMatch("aabbaa").getKey());

        t.put("http://xmlns.com/foaf/0.1/",true);
        t.put("http://xmlns.com/foaf/0.2/",true);
        t.put("http://xmlns.com/foaf/",true);
        t.put("http://xmlns.com/wordnet/1.6/",true);

        assertEquals("http://xmlns.com/foaf/0.1/",t.getLongestMatch("http://xmlns.com/foaf/0.1/Person").getKey());
        assertEquals("http://xmlns.com/foaf/0.2/",t.getLongestMatch("http://xmlns.com/foaf/0.2/Person").getKey());
        assertEquals("http://xmlns.com/foaf/",t.getLongestMatch("http://xmlns.com/foaf/Person").getKey());
        assertEquals("http://xmlns.com/wordnet/1.6/",t.getLongestMatch("http://xmlns.com/wordnet/1.6/Whatever").getKey());
    }

    @Test
    public void vocabFinderTest() {
        assertEquals("http://www.w3.org/ns/dcat#",VocabularyUtils.getVocabularyUrl("http://www.w3.org/ns/dcat#foo"));
        assertEquals("http://www.opengis.net/gml/",VocabularyUtils.getVocabularyUrl("http://www.opengis.net/gml/foo"));
        assertEquals("http://www.opengis.net/gml/",VocabularyUtils.getVocabularyUrl("http://www.opengis.net/gml/foo#bar"));
    }
}
