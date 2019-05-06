package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
@InterfaceAudience.Private
public class HHRawErasureCoderFactory implements RawErasureCoderFactory {
    public static final String CODER_NAME = "hitchhiker_java";
    @Override
    public String getCoderName() {
        return CODER_NAME;
    }

    @Override
    public String getCodecName() {
        return ErasureCodeConstants.Hitchhiker_CODEC_NAME;
    }

    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
        return new HHRawEncoder(coderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
        return new HHRawDecoder(coderOptions);
    }
}
