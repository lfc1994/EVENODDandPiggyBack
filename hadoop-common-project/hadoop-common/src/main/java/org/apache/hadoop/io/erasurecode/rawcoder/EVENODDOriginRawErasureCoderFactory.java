package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
@InterfaceAudience.Private
public class EVENODDOriginRawErasureCoderFactory implements RawErasureCoderFactory{
    public static final String CODER_NAME = "evenodd_origin_java";
    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
        return new EVENODDOriginRawEncoder(coderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
        return new EVENODDOriginRawDecoder(coderOptions);
    }

    @Override
    public String getCoderName() {
        return CODER_NAME;
    }

    @Override
    public String getCodecName() {
        return ErasureCodeConstants.EVENODD_CODEC_NAME;
    }
}
