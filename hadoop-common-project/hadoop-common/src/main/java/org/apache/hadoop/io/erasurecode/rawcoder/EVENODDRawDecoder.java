package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.EVENODDUtil;
import org.apache.hadoop.ipc.RpcWritable;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;

@InterfaceAudience.Private
public class EVENODDRawDecoder extends RawErasureDecoder {
    public EVENODDRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
    }
    protected void doDecode(ByteBufferDecodingState decodingState) {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        int unitLenth = decodingState.decodeLength/EVENODDUtil.rawSum;
        int[] erasuredIndex = decodingState.erasedIndexes;
        if (erasuredIndex.length ==1){   //单节点故障
            repairOneByXOR(decodingState.inputs,erasuredIndex[0]);
        }else {   //双节点故障
            if(erasuredIndex[1] == EVENODDUtil.columnSum){ //一个系统节点+第一个parity
                int preDestroyColumn = erasuredIndex[0];
                if (preDestroyColumn != 0){ //如果损坏的系统节点不是第一个，就需要先将该节点中的0先恢复到input中去
                    List<EVENODDUtil.EVENNode> helpZeroInColNodes = EVENODDUtil.preStepGetEvenOdd(preDestroyColumn);
                    int rawOfZeroInCol = EVENODDUtil.inverseEle(preDestroyColumn,0);
                    for (int byteIndex=0;byteIndex<unitLenth;byteIndex++){
                        caculateInInputBuffer(helpZeroInColNodes,decodingState.inputs,rawOfZeroInCol,preDestroyColumn,byteIndex,unitLenth,null);
                    }//把损坏的列中的0恢复出来
                }
                byte[] evenOdd = calculateSum(EVENODDUtil.calEvenOddNeedData(),unitLenth,decodingState.inputs);
                for (int byteIndex =0 ;byteIndex<unitLenth;byteIndex++){
                    for (int raw=0;raw<EVENODDUtil.rawSum;raw++){
                        int symbol = (raw+preDestroyColumn+1)%(EVENODDUtil.rawSum+1);
                        if(symbol ==0 ) continue;
                        if (symbol<EVENODDUtil.columnSum){ //需要evenodd因子
                            List<EVENODDUtil.EVENNode> nodes = EVENODDUtil.repairSymWithoutEvenOdd(preDestroyColumn,symbol);
                            caculateInInputBuffer(nodes,decodingState.inputs,raw,preDestroyColumn,byteIndex,unitLenth,evenOdd);
                        }else {//不需要evenodd因子
                            List<EVENODDUtil.EVENNode> nodes = EVENODDUtil.repairSymWithoutEvenOdd(preDestroyColumn,symbol);
                            caculateInInputBuffer(nodes,decodingState.inputs,raw,preDestroyColumn,byteIndex,unitLenth,null);
                        }
                    }
                }
                //恢复另一个
                repairOneByXOR(decodingState.inputs,erasuredIndex[1]);


            }else {  //两个系统节点损坏
                byte[] evenOdd = evenOddBy2Parity(decodingState.inputs,unitLenth);
                int preDestroyCol = decodingState.erasedIndexes[0];
                int nextDestroyCol = decodingState.erasedIndexes[1];
                int diff = nextDestroyCol-preDestroyCol;
                int beginSymbol = nextDestroyCol;//以第二列中缺失的符号为起点，而缺失的符号与列的索引刚好一致
                for (int i=0;i<EVENODDUtil.rawSum;i++){
                    int currentRaw = EVENODDUtil.inverseEle(preDestroyCol,beginSymbol);
                    List<EVENODDUtil.EVENNode> nodes = EVENODDUtil.repairSymWithoutEvenOdd(preDestroyCol,beginSymbol);//得到当前要修复的行
                    for (int byteIndex = 0; byteIndex<unitLenth;byteIndex++){
//                        计算该行的第一个损坏列的符号
                        if (beginSymbol<EVENODDUtil.columnSum){
                            caculateInInputBuffer(nodes,decodingState.inputs,currentRaw,preDestroyCol,byteIndex,unitLenth,evenOdd);
                        }else {
                            caculateInInputBuffer(nodes,decodingState.inputs,currentRaw,preDestroyCol,byteIndex,unitLenth,null);
                        }

                    }
//                    计算该行的第二个损坏列的符号
                    repairNextColAfterPreCol(decodingState.inputs,nextDestroyCol,currentRaw,unitLenth);
                    beginSymbol = (beginSymbol+diff)%5;
                }
            }
        }
    }

    /**
     * 根据坐标，计算出可被多次使用的中间结果
     * */
    private byte[] calculateSum (List<EVENODDUtil.EVENNode> nodes, int unitLength, ByteBuffer[] inputs){
        byte[] sum = new byte[unitLength];
        for (int i=0;i<unitLength;i++){
            for (int nodeIndex=0;nodeIndex<nodes.size();nodeIndex++){
                EVENODDUtil.EVENNode tempNode = nodes.get(nodeIndex);
                ByteBuffer tempBuffer = inputs[tempNode.y];
                byte temp = inputs[tempNode.y].get(tempBuffer.position()+tempNode.x*unitLength+i);
                sum[i] = (byte)(sum[i]^temp);
            }
        }
        return sum;
    }
    /**
     * 通过给出的EVENODD坐标节点，对bytebuffer的input中的字节进行相应的求和（可含EVENODD校验因子）
     * */
    private void caculateInInputBuffer(List<EVENODDUtil.EVENNode> nodes ,ByteBuffer[] input,int raw, int column,int indexInUnitbyte,int unitLength,byte[] evenOdd){
        byte temp = 0x00;
        for (EVENODDUtil.EVENNode node: nodes ) {
            temp = (byte)(temp^input[node.y].get(node.x*unitLength+indexInUnitbyte));//不算evenodd校验因子
        }
        if (evenOdd !=null){
            temp = (byte)(temp ^ evenOdd[indexInUnitbyte]);
        }
        input[column].put( raw*unitLength+indexInUnitbyte,temp);
    }

    /**
     * 单节点故障，直接异或处理，求出需要的数据
     * */
    private void repairOneByXOR(ByteBuffer[] inputs , int destroyColumn){
        int iIdx, oIdx;
        ByteBuffer output = inputs[destroyColumn];
        for (int i = 0; i <=EVENODDUtil.columnSum; i++) {
            // Skip the erased location.
            if (i == destroyColumn) {
                continue;
            }
            for (iIdx = inputs[i].position(), oIdx = output.position();
                 iIdx <inputs[i].limit();
                 iIdx++, oIdx++) {
                output.put(oIdx, (byte) (output.get(oIdx) ^
                        inputs[i].get(iIdx)));
            }
        }
    }
    /**
     * 当两个系统节点损坏的时候，一行中已经修复了一个的情况下， 修复另一个节点
     * */
    private void repairNextColAfterPreCol(ByteBuffer[] inputs,int nextDestroyCol,int currentRaw,int unitLength){
        int offset = currentRaw*unitLength;
        ByteBuffer output = inputs[nextDestroyCol];
        for (int i=0;i<=EVENODDUtil.columnSum;i++){
            if (i==nextDestroyCol){
                continue;
            }
            for (int byteIndex=0;byteIndex<unitLength;byteIndex++){
                output.put(offset+byteIndex,(byte)(inputs[i].get(offset+byteIndex)^output.get(offset+byteIndex)));
            }
        }

    }
    /**
     * 双系统节点损坏时，根据两个校验列，求得evenodd校验因子
     * */
    private byte[] evenOddBy2Parity (ByteBuffer[] inputs , int unitLength){
        byte[] temp = new byte[unitLength];
        int par1Column = EVENODDUtil.columnSum;
        int par2Column = par1Column+1;
        for (int raw =0 ;raw <EVENODDUtil.rawSum; raw++){
            int offset = raw*unitLength;
            for (int byteIndex=0;byteIndex<unitLength;byteIndex++){
                temp[byteIndex] = (byte)(temp[byteIndex]^inputs[par1Column].get(offset+byteIndex)
                        ^inputs[par2Column].get(offset+byteIndex));
            }
        }
        return temp;
    }

    protected void doDecode(ByteArrayDecodingState decodingState) {
        byte[] output = decodingState.outputs[0];
        int dataLen = decodingState.decodeLength;
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.outputOffsets, dataLen);
        int erasedIdx = decodingState.erasedIndexes[0];

        // Process the inputs.
        int iIdx, oIdx;
        for (int i = 0; i < decodingState.inputs.length; i++) {
            // Skip the erased location.
            if (i == erasedIdx) {
                continue;
            }

            for (iIdx = decodingState.inputOffsets[i],
                         oIdx = decodingState.outputOffsets[0];
                 iIdx < decodingState.inputOffsets[i] + dataLen; iIdx++, oIdx++) {
                output[oIdx] ^= decodingState.inputs[i][iIdx];
            }
        }
    }
}

