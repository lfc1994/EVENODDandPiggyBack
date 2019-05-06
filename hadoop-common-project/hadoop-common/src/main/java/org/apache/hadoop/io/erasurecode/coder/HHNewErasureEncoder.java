package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class HHNewErasureEncoder extends EVENODDErasureEncoder{
    public HHNewErasureEncoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareEncodingStep(ECBlockGroup blockGroup) {
        RawErasureEncoder rawEncoder = CodecUtil.createRawEncoder(getConf(),
                ErasureCodeConstants.HitchhikerNew_CODEC_Name, getOptions());

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureEncodingStep(inputBlocks,
                getOutputBlocks(blockGroup), rawEncoder);
    }
}
