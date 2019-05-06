package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.io.IOException;
import java.nio.ByteBuffer;

@InterfaceAudience.Private
public class EVENODDOriginRawEncoder extends RawErasureEncoder {
    public EVENODDOriginRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
    }

    @Override
    protected void doEncode(ByteBufferEncodingState encodingState) throws IOException {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.encodeLength);
        ByteBuffer output = encodingState.outputs[0];//第一个校验节点
        int dataLength = encodingState.encodeLength;
        int unitLength = dataLength/4;
        //计算第一个校验节点
        int iIdx, oIdx;
        for (iIdx = encodingState.inputs[0].position(), oIdx = output.position();
             iIdx < encodingState.inputs[0].limit(); iIdx++, oIdx++) {
            output.put(oIdx, encodingState.inputs[0].get(iIdx));
        }
        for (int i = 1; i < encodingState.inputs.length; i++) {
            for (iIdx = encodingState.inputs[i].position(), oIdx = output.position();
                 iIdx < encodingState.inputs[i].limit();
                 iIdx++, oIdx++) {
                output.put(oIdx, (byte) (output.get(oIdx) ^
                        encodingState.inputs[i].get(iIdx)));
            }
        }
        byte[] e = new byte[unitLength];
        for (int i=0;i<unitLength;i++){
            e[i] =(byte)(encodingState.inputs[1].get(i+3*unitLength)^encodingState.inputs[2].get(i+2*unitLength));
        }
        for (int i=0;i<unitLength;i++){
            byte one = (byte)(e[i]^encodingState.inputs[0].get(i)^encodingState.inputs[2].get(i+3*unitLength));
            encodingState.outputs[1].put(i,one);
            byte two = (byte)(e[i]^encodingState.inputs[0].get(i+unitLength)^encodingState.inputs[1].get(i));
            encodingState.outputs[1].put(i+unitLength,two);
            byte three = (byte)(e[i]^encodingState.inputs[0].get(i+2*unitLength)^encodingState.inputs[1].get(i+unitLength)^encodingState.inputs[2].get(i));
            encodingState.outputs[1].put(i+2*unitLength,three);
            byte four = (byte)(e[i]^encodingState.inputs[0].get(i+3*unitLength)^encodingState.inputs[1].get(i+2*unitLength)^encodingState.inputs[2].get(i+unitLength));
            encodingState.outputs[1].put(i+3*unitLength,four);
        }
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) throws IOException {
        int dataLength = encodingState.encodeLength;
        ByteBuffer[] inputBuffers = new ByteBuffer[encodingState.inputs.length];
        ByteBuffer[] outputBuffers = new ByteBuffer[encodingState.outputs.length];
        for (int i=0;i<inputBuffers.length;i++){
            inputBuffers[i] = ByteBuffer.wrap(encodingState.inputs[i],encodingState.inputOffsets[i],dataLength);
        }
        for (int j=0;j<outputBuffers.length;j++){
            outputBuffers[j] = ByteBuffer.wrap(encodingState.outputs[j],encodingState.outputOffsets[j],dataLength);
        }
        ByteBufferEncodingState byteBufferEncodingState = new ByteBufferEncodingState(encodingState.encoder,inputBuffers,outputBuffers);
        doEncode(byteBufferEncodingState);
    }
}
