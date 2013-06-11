package de.wbsg.loddesc.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;


public class VocabularyUtils {
    private static Logger log = Logger.getLogger(VocabularyUtils.class);
    private static Trie<Boolean> wellKnownVocabs = null;

    public static String getVocabularyUrl(String someUrl) {
        // okay, first check our trie, maybe we are lucky
        String vocab = getWellKnownVocab(someUrl);
        if (vocab != null) {
            return vocab;
        }
        // not? then try to split away stuff behind # and /
        return getUrlPrefix(someUrl);
    }
    

    public static String getWellKnownVocab(String someUrl) {
         if (wellKnownVocabs == null) {
             wellKnownVocabs = loadPrefixes();
         }
        Map.Entry<CharSequence,Boolean> entry =  wellKnownVocabs.getLongestMatch(someUrl);
        if (entry == null) {
            return null;
        }
        return entry.getKey().toString();
    }

    public static String getUrlPrefix(String r) {
         if (r.lastIndexOf("#") > -1) {
            return r.substring(0, r.lastIndexOf("#") + 1);
        }
        if (r.lastIndexOf("/") > -1) {
            return r.substring(0, r.lastIndexOf("/") + 1);
        }
        return r;
    }

    private static Trie<Boolean> loadPrefixes() {
        Trie<Boolean> t = new HashTrie<Boolean>();
        try {
            InputStream is = VocabularyUtils.class.getClassLoader().getResourceAsStream("vocabularyUrls.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                if (strLine.trim().equals(""))     {
                    continue;
                }
                t.put(strLine, true);
            }
            is.close();
        } catch (Exception e) {
            log.warn("Error loading known vocabulary prefixes: " + e.getMessage());
        }
        return t;
    }


}
