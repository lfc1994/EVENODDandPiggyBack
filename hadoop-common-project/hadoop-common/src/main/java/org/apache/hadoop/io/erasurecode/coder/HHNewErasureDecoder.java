package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

public class HHNewErasureDecoder extends EVENODDErasureDecoder {
    public HHNewErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(ECBlockGroup blockGroup) {
        RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(getConf(),
                ErasureCodeConstants.HitchhikerNew_CODEC_Name, getOptions());

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureDecodingStep(inputBlocks,
                getErasedIndexes(inputBlocks),
                getOutputBlocks(blockGroup), rawDecoder);
    }
}
