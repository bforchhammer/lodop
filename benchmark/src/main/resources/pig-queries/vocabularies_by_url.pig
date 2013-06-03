-- Vocabulary Usage By URL

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

predicateVocabs = FOREACH quads GENERATE de.wbsg.loddesc.functions.Vocab(predicate) AS vocab, graph;
typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
objectVocabs = FOREACH typedstuff GENERATE de.wbsg.loddesc.functions.Vocab(object.value) AS vocab, graph;
allVocabsUrls = UNION predicateVocabs, objectVocabs;
vocabsUrls = DISTINCT allVocabsUrls;
vocabs = GROUP vocabsUrls BY vocab;
vocabCounts = FOREACH vocabs GENERATE group, COUNT(vocabsUrls) AS cnt;
topVocabsEntities = ORDER vocabCounts BY cnt DESC;