package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.EVENODDUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

@InterfaceAudience.Private
public class EVENODDOriginRawDecoder extends RawErasureDecoder {
    public EVENODDOriginRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
    }

    @Override
    protected void doDecode(ByteBufferDecodingState decodingState) throws IOException {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        int[] erasuredIndex = decodingState.erasedIndexes;
        //为了适配evenodd函数而做的一些辅助性操作
        int outPutIndex = 0;
        System.out.println("decodingState.inputs.length");
        System.out.println(decodingState.inputs.length);
        for (int j=0;j<decodingState.inputs.length;j++){
            System.out.println("j="+j);
            if (decodingState.inputs[j]==null){
                System.out.println("enter into null judge, where once throw ArrayIndexOutOfBound");
                decodingState.inputs[j]=decodingState.outputs[outPutIndex];
                outPutIndex++;
            }
        }
        EVENODDRawDecoder.repairOneByXOR(decodingState.inputs,erasuredIndex[0]);
    }

    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) throws IOException {
        int decodeLenth = decodingState.decodeLength;
        ByteBuffer[] outputByteBuffers = new ByteBuffer[decodingState.outputs.length];
        ByteBuffer[] inputByteBuffers = new ByteBuffer[decodingState.inputs.length];
        for (int i=0;i<outputByteBuffers.length;i++){
            outputByteBuffers[i] = ByteBuffer.wrap(decodingState.outputs[i],decodingState.outputOffsets[i],decodeLenth);
        }
        int indexInerasureArray = 0;
        for (int j=0;j<inputByteBuffers.length;j++){
            //此处有些部分在前文有些坏掉的erasedIndexes是设置为null的，所以会有空指针异常！！！
            if(indexInerasureArray <= decodingState.erasedIndexes.length-1){
                if (j==decodingState.erasedIndexes[indexInerasureArray]){
                    inputByteBuffers[j] =null;
                    indexInerasureArray++;
                    continue;
                }
            }
            inputByteBuffers[j] = ByteBuffer.wrap(decodingState.inputs[j],decodingState.inputOffsets[j],decodeLenth);
        }
        ByteBufferDecodingState byteBufferDecodingState = new ByteBufferDecodingState(decodingState.decoder,decodeLenth,decodingState.erasedIndexes,inputByteBuffers,outputByteBuffers);
        doDecode(byteBufferDecodingState);
    }
}
