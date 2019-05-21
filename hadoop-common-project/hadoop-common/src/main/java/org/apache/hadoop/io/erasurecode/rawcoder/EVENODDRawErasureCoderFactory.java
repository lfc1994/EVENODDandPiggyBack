package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * A raw coder factory for the raw EVENODD coder in Java.
 */
@InterfaceAudience.Private
public class EVENODDRawErasureCoderFactory implements RawErasureCoderFactory {

    public static final String CODER_NAME = "evenodd_java";

    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
        return new RSRawEncoder(coderOptions);
//        return new EVENODDRawEncoder(coderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
        return new RSRawDecoder(coderOptions);
//        return new EVENODDRawDecoder(coderOptions);
    }

    @Override
    public String getCoderName() {
        return CODER_NAME;
    }

    @Override
    public String getCodecName() {
        return ErasureCodeConstants.EVENODDPlus_CODEC_NAME;
    }
}
