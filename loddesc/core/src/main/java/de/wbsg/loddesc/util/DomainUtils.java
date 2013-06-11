package de.wbsg.loddesc.util;

import com.google.common.net.InternetDomainName;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DomainUtils {
    private static Logger log = Logger.getLogger(DomainUtils.class);

    public static String getPayLevelDomain(String domain) {
        try {
            InternetDomainName fullDomainName = InternetDomainName.from(domain);
            return fullDomainName.topPrivateDomain().name();
        } catch (Exception e) {
            log.warn(domain, e);
        }
        return domain;
    }

    private static final Pattern DOMAIN_PATTERN = Pattern
            .compile("http(s)?://(([a-zA-Z0-9-]+(\\.)?)+)");

    public static String getDomain(String uri) {
        try {
            Matcher m = DOMAIN_PATTERN.matcher(uri);
            if (m.find()) {
                return m.group(2);
            }
        } catch (Exception e) {
            log.warn(uri, e);
        }
        return uri;
    }

    public static String getTopLevelDomain(String domain) {
        try {
            InternetDomainName fullDomainName = InternetDomainName.from(domain);
            return fullDomainName.publicSuffix().name();
        } catch (Exception e) {
            log.warn(domain, e);
        }
        return domain;
    }
}
