package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class EVENODDOriginErasureEncoder extends ErasureEncoder {
    public EVENODDOriginErasureEncoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareEncodingStep(ECBlockGroup blockGroup) {
            RawErasureEncoder rawEncoder = CodecUtil.createRawEncoder(getConf(),
                    ErasureCodeConstants.EVENODD_CODEC_NAME, getOptions());

            ECBlock[] inputBlocks = getInputBlocks(blockGroup);

            return new ErasureEncodingStep(inputBlocks,
                    getOutputBlocks(blockGroup), rawEncoder);
        }

}
