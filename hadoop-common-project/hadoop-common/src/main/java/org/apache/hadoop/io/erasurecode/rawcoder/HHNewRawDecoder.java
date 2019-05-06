package org.apache.hadoop.io.erasurecode.rawcoder;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.GF256;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
/**
 * 针对(9,6)Hitchhiker-XOR(new)
 * */
@InterfaceAudience.Private
public class HHNewRawDecoder extends RawErasureDecoder{
    private byte[] encodeMatrix;

    /**
     * Below are relevant to schema and erased indexes, thus may change during
     * decode calls.
     */
    private byte[] decodeMatrix;
    private byte[] invertMatrix;
    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    private byte[] gfTablesToDecode;
    private byte[] gfTablesToEncode;
    private int[] cachedErasedIndexes;//之所以使用一个缓存数组是为了减少家吗矩阵的生成次数
    private int[] validIndexes;//存放输入的数组的有效的index
    private int numErasedDataUnits;
    private boolean[] erasureFlags;

    public HHNewRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        int numAllUnits = getNumAllUnits();
        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid getNumDataUnits() and numParityUnits");
        }

        encodeMatrix = new byte[numAllUnits * getNumDataUnits()];
        RSUtil.genCauchyMatrix(encodeMatrix, numAllUnits, getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), numAllUnits);
        }
        gfTablesToEncode = new byte[getNumAllUnits() * getNumDataUnits() * 32];
        RSUtil.initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
                getNumDataUnits() * getNumDataUnits(), gfTablesToEncode);
    }

    /**
     * 只考虑损坏的是第一个
     * */
    @Override
    protected void doDecode(ByteBufferDecodingState decodingState) throws IOException {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);//初始化output的buffer的长度，至少得有4KB
        int unitLength = decodingState.decodeLength/4;
        prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);
        ByteBuffer[] realInputs = new ByteBuffer[getNumDataUnits()];
        for (int i = 0; i < getNumDataUnits(); i++) {
            realInputs[i] = decodingState.inputs[validIndexes[i]];//录入解码需要的数据
        }
        for (int i=0;i<getNumDataUnits();i++){
            realInputs[i].position(unitLength);
            realInputs[i].limit(2*unitLength);
        }
        for(int i=0;i<decodingState.outputs.length;i++){
            decodingState.outputs[i].position(unitLength);
            decodingState.outputs[i].limit(2*unitLength);
        }
        RSUtil.encodeData(gfTablesToDecode, realInputs, decodingState.outputs);   //恢复b1
        for (int i=0;i<getNumDataUnits();i++){
            realInputs[i].position(3*unitLength);
            realInputs[i].limit(4*unitLength);
        }
        for(int i=0;i<decodingState.outputs.length;i++){
            decodingState.outputs[i].position(3*unitLength);
            decodingState.outputs[i].limit(4*unitLength);
        }
        RSUtil.encodeData(gfTablesToDecode, realInputs, decodingState.outputs);  //恢复d1
        ByteBuffer[] inputsToEncodef2 = new ByteBuffer[getNumDataUnits()];
        ByteBuffer[] encodefBytebuffer =  new ByteBuffer[getNumParityUnits()];
        //初始化校验数据f1(b),f2(b),f3(b)，防止空指针异常
        for (int i=0;i<getNumParityUnits();i++){
         byte[] tmp = new byte[decodingState.decodeLength];
            encodefBytebuffer[i] = ByteBuffer.wrap(tmp);
        }
        inputsToEncodef2[0] = decodingState.outputs[0];
        for (int i=1;i<getNumDataUnits();i++){
            inputsToEncodef2[i] = realInputs[i-1];
        }
        for (int i=0;i<getNumDataUnits();i++){
            inputsToEncodef2[i].position(unitLength);
            inputsToEncodef2[i].limit(2*unitLength);
        }
        for (int i=0;i<getNumParityUnits();i++){
            encodefBytebuffer[i].position(unitLength);
            encodefBytebuffer[i].limit(2*unitLength);
        }
        RSUtil.encodeData(gfTablesToEncode, inputsToEncodef2, encodefBytebuffer); //计算f2(b)
        for (int i=0;i<getNumDataUnits();i++){
            inputsToEncodef2[i].position(3*unitLength);
            inputsToEncodef2[i].limit(4*unitLength);
        }
        for (int i=0;i<getNumParityUnits();i++){
            encodefBytebuffer[i].position(3*unitLength);
            encodefBytebuffer[i].limit(4*unitLength);
        }
        RSUtil.encodeData(gfTablesToEncode, inputsToEncodef2, encodefBytebuffer); //计算f2(d)
        for (int i=0;i<unitLength;i++){
            //a2,f2(b),f2(b)+a1+a2 计算a1
            byte a1 = (byte)(realInputs[0].get(i)^encodefBytebuffer[1].get(i+unitLength)^decodingState.inputs[7].get(i+unitLength));
            decodingState.outputs[0].put(i,a1);
            //c2,b4,b5,f2(d),f2(d)+c1+c2+b4+b5 计算c1
            byte c1 = (byte)(realInputs[0].get(i+2*unitLength)^realInputs[2].get(i+unitLength)^realInputs[3].get(i+unitLength)^encodefBytebuffer[1].get(i+3*unitLength)^decodingState.inputs[7].get(i+3*unitLength));
            decodingState.outputs[0].put(i+2*unitLength,c1);
        }
        //将position和limit归位；
        for (int i=0;i<getNumDataUnits();i++){
            realInputs[i].position(0);
            realInputs[i].limit(4*unitLength);
        }
        decodingState.outputs[0].position(0);
        decodingState.outputs[0].limit(4*unitLength);
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
    private <T> void prepareDecoding(T[] inputs, int[] erasedIndexes) {
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);//存放输入的数组的有效的位置
        if (Arrays.equals(this.cachedErasedIndexes, erasedIndexes) &&
                Arrays.equals(this.validIndexes, tmpValidIndexes)) {
            return; // Optimization. Nothing to do
        }
        this.cachedErasedIndexes =
                Arrays.copyOf(erasedIndexes, erasedIndexes.length);
        this.validIndexes =
                Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);

        processErasures(erasedIndexes);
    }

    private void processErasures(int[] erasedIndexes) {
        this.decodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        this.invertMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        this.gfTablesToDecode = new byte[getNumAllUnits() * getNumDataUnits() * 32];

        this.erasureFlags = new boolean[getNumAllUnits()];
        this.numErasedDataUnits = 0;

        for (int i = 0; i < erasedIndexes.length; i++) {
            int index = erasedIndexes[i];
            erasureFlags[index] = true;
            if (index < getNumDataUnits()) {
                numErasedDataUnits++;
            }
        }

        generateDecodeMatrix(erasedIndexes);

        RSUtil.initTables(getNumDataUnits(), erasedIndexes.length,
                decodeMatrix, 0, gfTablesToDecode);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTablesToDecode, -1));
        }
    }

    private void generateDecodeMatrix(int[] erasedIndexes) {
        int i, j, r, p;
        byte s;
        byte[] tmpMatrix = new byte[getNumAllUnits() * getNumDataUnits()];

        // Construct matrix tmpMatrix by removing error rows
        //重新构造一个k*k的矩阵，选择的是k*m生成矩阵的前k个健康的节点所代表的行
        for (i = 0; i < getNumDataUnits(); i++) {
            r = validIndexes[i];
            for (j = 0; j < getNumDataUnits(); j++) {
                tmpMatrix[getNumDataUnits() * i + j] =
                        encodeMatrix[getNumDataUnits() * r + j];
            }
        }

        GF256.gfInvertMatrix(tmpMatrix, invertMatrix, getNumDataUnits()); //求逆矩阵

        for (i = 0; i < numErasedDataUnits; i++) {
            for (j = 0; j < getNumDataUnits(); j++) {
                decodeMatrix[getNumDataUnits() * i + j] =
                        invertMatrix[getNumDataUnits() * erasedIndexes[i] + j];//生成的逆矩阵是一个满秩的矩阵，选出那几个要修复的行，用于恢复出缺失的系统数据
            }
        }

        for (p = numErasedDataUnits; p < erasedIndexes.length; p++) {
            for (i = 0; i < getNumDataUnits(); i++) {
                s = 0;
                for (j = 0; j < getNumDataUnits(); j++) {
                    s ^= GF256.gfMul(invertMatrix[j * getNumDataUnits() + i],
                            encodeMatrix[getNumDataUnits() * erasedIndexes[p] + j]);
                }
                decodeMatrix[getNumDataUnits() * p + i] = s;//上面补充的是恢复系统数据需要的行，此处补充的是要恢复的校验数据需要的行，用于恢复缺失的校验数据
            }
        }
    }

}
