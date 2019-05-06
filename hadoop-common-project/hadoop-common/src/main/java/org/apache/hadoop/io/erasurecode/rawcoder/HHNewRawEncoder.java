package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
@InterfaceAudience.Private
public class HHNewRawEncoder extends RawErasureEncoder{
    private byte[] encodeMatrix;
    private byte[] gfTables;
    public HHNewRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid numDataUnits and numParityUnits");
        }

        encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        RSUtil.genCauchyMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), getNumAllUnits());
        }
        gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];
        RSUtil.initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
                getNumDataUnits() * getNumDataUnits(), gfTables);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }
    }

    @Override
    protected void doEncode(ByteBufferEncodingState encodingState) throws IOException {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.encodeLength);
        RSUtil.encodeData(gfTables, encodingState.inputs, encodingState.outputs);
        int dataLength = encodingState.encodeLength;
        int unitLength = dataLength/4;
        int symbolbOffset = unitLength;
        int symbolcOffset = 2*unitLength;
        int symboldOffset = 3*unitLength;
        for (int i=0;i<unitLength;i++){
            //计算f2(b)+a1+a2
            encodingState.outputs[1].put(symbolbOffset+i,
                    (byte)(encodingState.outputs[1].get(symbolbOffset+i)^encodingState.inputs[0].get(i)^encodingState.inputs[1].get(i)));
            //计算f3(b)+a3
            encodingState.outputs[2].put(symbolbOffset+i,
                    (byte)(encodingState.outputs[2].get(symbolbOffset+i)^encodingState.inputs[2].get(i)));
            //计算f2(c)+a4+a5
            encodingState.outputs[1].put(symbolcOffset+i,
                    (byte)(encodingState.outputs[1].get(symbolcOffset+i)^encodingState.inputs[3].get(i)^encodingState.inputs[4].get(i)));
            //计算f3(c)+a6
            encodingState.outputs[2].put(symbolcOffset+i,
                    (byte)(encodingState.outputs[2].get(symbolcOffset+i)^encodingState.inputs[5].get(i)));
            //计算f2(d)+c1+c2+b4+b5
            encodingState.outputs[1].put(symboldOffset+i,
                    (byte)(encodingState.outputs[1].get(symboldOffset+i)^encodingState.inputs[0].get(symbolcOffset+i)^encodingState.inputs[1].get(symbolcOffset+i)^
                            encodingState.inputs[3].get(symbolbOffset+i)^encodingState.inputs[4].get(symbolbOffset+i)));
            //计算f3(d)+c3+b6
            encodingState.outputs[2].put(symboldOffset+i,
                    (byte)(encodingState.outputs[2].get(symboldOffset+i)^encodingState.inputs[2].get(symbolcOffset+i)^encodingState.inputs[5].get(symbolbOffset+i)));
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
