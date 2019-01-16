package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.EVENODDUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * 这是一个(5-1)*(3+2)的一个EVENODD+的编码
 **/
@InterfaceAudience.Private
public class EVENODDRawEncoder extends RawErasureEncoder {
    public EVENODDRawEncoder(ErasureCoderOptions coderOptions) {
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
        //计算第二个校验节点
        List <List<EVENODDUtil.EVENNode>> nodesRepair2Par = EVENODDUtil.repairPar2();
        for (int raw=0;raw<nodesRepair2Par.size();raw++){
            int outPutOffset = raw * unitLength;
            List<EVENODDUtil.EVENNode> nodesRepair1Raw = nodesRepair2Par.get(raw);
            for (EVENODDUtil.EVENNode node: nodesRepair1Raw) {
                int inPutOffset = node.x * unitLength;
                for (int byteIndex =0;byteIndex<unitLength;byteIndex++){
                    byte temp = (byte) (encodingState.outputs[1].get(outPutOffset+byteIndex)^encodingState.inputs[node.y].get(inPutOffset+byteIndex));
                    encodingState.outputs[1].put(outPutOffset+byteIndex,temp);
                }
            }
        }
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) throws IOException {
    }
}
