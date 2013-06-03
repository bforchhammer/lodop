-- Vocabulary Usage By Top Level Domain

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

predicateVocabs = FOREACH quads GENERATE de.wbsg.loddesc.functions.Vocab(predicate) AS vocab, de.wbsg.loddesc.functions.PayLevelDomain(graph) AS pld;
typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
objectVocabs = FOREACH typedstuff GENERATE de.wbsg.loddesc.functions.Vocab(object.value) AS vocab, de.wbsg.loddesc.functions.PayLevelDomain(graph) AS pld;
allVocabsTlds = UNION predicateVocabs, objectVocabs;
vocabsAndTlds = DISTINCT allVocabsTlds;
tlds = GROUP vocabsAndTlds BY (de.wbsg.loddesc.functions.TopLevelDomain(pld),vocab);
tldTypeCounts = FOREACH tlds GENERATE FLATTEN(group) AS (tld,vocab), COUNT(vocabsAndTlds) AS cnt;
topClassesTld = ORDER tldTypeCounts BY tld, cnt DESC;