package org.apache.hadoop.io.erasurecode.rawcoder.util;

import java.util.ArrayList;
import java.util.List;

/**
 * (5-1)*(3+2)的EVENODD+生成矩阵(生成第二个校验节点),列代表节点！
 * */
public class EVENODDUtil {
    public static int[][] evenOddMetrix = {{1,2,3},
            {2,3,4},
            {3,4,0},
            {4,0,1}};
    public static int columnSum =evenOddMetrix[0].length; //3
    public static int rawSum = evenOddMetrix.length;  //4
    public static class EVENNode{
        public int x;
        public int y;
        public EVENNode(int x,int y){
            this.x = x;
            this.y = y;
        }
    }
    /**
     *第二个校验节点编码准则：
     * @return 修复第二个节点，返回一个二维list，最外层的list的索引对应最后一个节点层数
     */
    public static List<List<EVENNode>> repairPar2(){
        List<List<EVENNode>> nodes = new ArrayList<>();
        for (int i=0;i<evenOddMetrix.length;i++){
            nodes.add(new ArrayList<>()); //初始化，否则会报空指针异常
        }
        List<EVENNode> evenOddList = new ArrayList<>();
        int numData = evenOddMetrix[0].length;
        int layers = evenOddMetrix.length;
        for (int symbol=0;symbol<numData;symbol++ ){
            if (symbol == 0){
                for ( int column=1;column<numData;column++){
                    int currentRaw = inverseEle(column,0);
                    evenOddList.add(new EVENNode(currentRaw,column));
                }
            }else {
                for (int column = 0; column < numData; column++) {
                    if (column == symbol) {
                        continue;
                    }
                    int currentRaw = inverseEle(column, symbol);
                    nodes.get(symbol - 1).add(new EVENNode(currentRaw, column));
                }
                nodes.get(symbol - 1).addAll(evenOddList);
            }
        }
        for (int symbol=numData;symbol<=layers;symbol++){
            for (int column=0;column<numData;column++){
                int currentRaw = inverseEle(column,symbol);
                nodes.get(symbol-1).add(new EVENNode(currentRaw,column));
            }
        }
        return nodes;
    }
    /**(已验证)
     *  一系统节点+一个校验节点损坏：
     *  恢复错误节点中的0需要的数据列表，同时，相当于得到了EVENODD的校验值
     * @param destoryColumn 损坏的系统节点在stripe中的偏移量  1<=destoryColumn<=numData-1
     * @return  恢复失败节点中的0，需要的数据
     * */
    public static List<EVENNode> preStepGetEvenOdd(int destoryColumn){
        List<EVENNode> nodes = new ArrayList<>();
        for (int column = 0; column < columnSum; column++) {
            if (column != destoryColumn) {
                int currentRaw = inverseEle(column, destoryColumn);
                nodes.add(new EVENNode(currentRaw, column));
            }
            if (column != destoryColumn && column != 0) {
                int currentRaw = inverseEle(column, 0);
                nodes.add(new EVENNode(currentRaw, column));
            }
        }
        nodes.add(new EVENNode(destoryColumn - 1, columnSum + 1));//加上第二个校验节点需要的数据
        return nodes;
    }
    /**
     * 修复一列中该符号关联的数据坐标，该方法不会消去evenodd因子，任需手动消除
     * */
    public static List<EVENNode> repairSymWithoutEvenOdd(int destroyColumn , int symbol){
        List<EVENNode> nodes = new ArrayList<>();
        for (int column=0;column<columnSum;column++){
            if(column != destroyColumn){
                int raw = inverseEle(column,symbol);
                if (raw<rawSum){
                    nodes.add(new EVENNode(raw,column));
                }
            }
        }
        if (symbol !=0) {  //因为在拥有evenodd因子的情况下，第二个校验列的数据
            nodes.add(new EVENNode(symbol-1,columnSum+1));
        }
        return nodes;
    }
    /**
     * 计算evenodd校验因子需要的数据坐标
     * */
    public static  List<EVENNode> calEvenOddNeedData(){
        List<EVENNode> nodes = new ArrayList<>();
        for (int i=1;i<columnSum;i++){
            int raw = inverseEle(i,0);
            nodes.add(new EVENNode(raw,i));
        }
        return nodes;
    }




    /**
     * 求加法逆元
     * @param column 代表节点序号
     * @param symbol 代表evenOddMetrix中的数字
     * @return evenOddMetrix中的数字对应的行号
     * */
    public static int inverseEle (int column ,int symbol){
        int model = evenOddMetrix.length+1;
        int raw = symbol - column -1;
        if (raw < 0){
            raw += model;
        }
        return raw;
    }
}
