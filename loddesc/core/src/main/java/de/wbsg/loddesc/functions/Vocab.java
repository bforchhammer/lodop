package de.wbsg.loddesc.functions;

import de.wbsg.loddesc.util.VocabularyUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class Vocab extends EvalFunc<String> {

    @Override
    public String exec(Tuple arg0) throws IOException {
        return VocabularyUtils.getVocabularyUrl((String) arg0.get(0));
    }
}
