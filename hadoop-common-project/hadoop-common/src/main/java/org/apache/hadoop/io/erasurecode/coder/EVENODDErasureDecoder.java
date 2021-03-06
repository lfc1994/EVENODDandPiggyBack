package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class EVENODDErasureDecoder extends ErasureDecoder{

    public EVENODDErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(
            final ECBlockGroup blockGroup) {
        RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(getConf(),
                ErasureCodeConstants.EVENODDPlus_CODEC_NAME, getOptions());

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureDecodingStep(inputBlocks,
                getErasedIndexes(inputBlocks),
                getOutputBlocks(blockGroup), rawDecoder);
    }

}
