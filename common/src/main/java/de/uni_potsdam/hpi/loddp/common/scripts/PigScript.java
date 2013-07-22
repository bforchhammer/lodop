package de.uni_potsdam.hpi.loddp.common.scripts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A pig script.
 */
public abstract class PigScript {

    /**
     * Returns a new InputStream for wrapped pig script file.
     *
     * @return InputStream instance.
     */
    public abstract InputStream getNewInputStream();

    /**
     * Returns the filename of the pig script (without the extension).
     *
     * @return the name of the pig script.
     */
    public abstract String getScriptName();

    /**
     * Returns the contents of this script by completely reading the input stream from {@link #getNewInputStream()}.
     *
     * @return the contents of the script.
     */
    public String getContent() {
        StringBuilder sb = new StringBuilder();
        BufferedReader in = new BufferedReader(new InputStreamReader(getNewInputStream()));
        String line = null;
        try {
            while ((line = in.readLine()) != null) {
                sb.append(line).append('\n');
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("PigScript[%s]", getScriptName());
    }

}
