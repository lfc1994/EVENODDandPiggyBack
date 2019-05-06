package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

@InterfaceAudience.Private
public class HHNewRawErasureCoderFactory implements RawErasureCoderFactory{
    public static final String CODER_NAME = "hitchhiker_new_java";

    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
        return new HHNewRawEncoder(coderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
        return new HHNewRawDecoder(coderOptions);
    }

    @Override
    public String getCoderName() {
        return CODER_NAME;
    }

    @Override
    public String getCodecName() {
        return ErasureCodeConstants.HitchhikerNew_CODEC_Name;
    }
}
