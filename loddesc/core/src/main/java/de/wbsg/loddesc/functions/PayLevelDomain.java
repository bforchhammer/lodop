package de.wbsg.loddesc.functions;

import de.wbsg.loddesc.util.DomainUtils;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class PayLevelDomain extends EvalFunc<String> {
	private static Logger log = Logger.getLogger(PayLevelDomain.class);

	@Override
	public String exec(Tuple arg0) throws IOException {
		return DomainUtils.getPayLevelDomain(DomainUtils.getDomain((String) arg0.get(0)));
	}


}
