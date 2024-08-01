package cn.edu.zju.daily.data.result;

import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;

public class SearchResultEncoder implements Encoder<SearchResult> {

    private final String charsetName;

    /**
     * Creates a new {@code SearchResultEncoder} that uses {@code "UTF-8"} charset to convert strings to
     * bytes.
     */
    public SearchResultEncoder() {
        this("UTF-8");
    }

    /**
     * Creates a new {@code SearchResultEncoder} that uses the given charset to convert strings to bytes.
     *
     * @param charsetName Name of the charset to be used, must be valid input for {@code
     *                    Charset.forName(charsetName)}
     */
    public SearchResultEncoder(String charsetName) {
        this.charsetName = charsetName;
    }

    @Override
    public void encode(SearchResult element, OutputStream stream) throws IOException {

        StringBuilder sb = new StringBuilder();
        sb.append(element.getQueryId()).append(" ").append(element.getSearchDuration());
        for (int i = 0; i < element.size(); i++) {
            sb.append(" ").append(element.id(i)).append(",").append(element.distance(i));
        }
        sb.append("\n");
        stream.write(sb.toString().getBytes(charsetName));
    }
}
