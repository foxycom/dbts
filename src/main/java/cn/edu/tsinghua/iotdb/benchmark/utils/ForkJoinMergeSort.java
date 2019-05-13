package cn.edu.tsinghua.iotdb.benchmark.utils;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;


public class ForkJoinMergeSort {

  public int[] sort(int[] rawArray, int parallelism) {
    ForkJoinPool pool = new ForkJoinPool(parallelism);
    pool.invoke(new MergeSort(rawArray));
    return rawArray;
  }

  /**
   * 使用Fork/join的方式进行归并排序，充分利用cpu
   */
  private static class MergeSort extends RecursiveAction {

    private static final long serialVersionUID = 425572392953885545L;
    private int[] intArr;

    public MergeSort(int[] intArr) {
      this.intArr = intArr;
    }

    @Override
    protected void compute() {
      if (intArr.length > 1) {
        // 如果数组长度大于1就分解称两份
        int[] leftArray = Arrays.copyOfRange(intArr, 0, intArr.length / 2);
        int[] rightArray = Arrays.copyOfRange(intArr, intArr.length / 2, intArr.length);

        // 这里分成两份执行
        invokeAll(new MergeSort(leftArray), new MergeSort(rightArray));

        // 合并且排序
        merge(leftArray, rightArray, intArr);
      }
    }

    /**
     * 合并排序
     */
    private void merge(int[] leftArray, int[] rightArray, int[] intArr) {

      // i：leftArray数组索引，j：rightArray数组索引，k：intArr数组索引
      int i = 0, j = 0, k = 0;
      while (i < leftArray.length && j < rightArray.length) {
        // 当两个数组中都有值的时候，比较当前元素进行选择
        if (leftArray[i] < rightArray[j]) {
          intArr[k] = leftArray[i];
          i++;
        } else {
          intArr[k] = rightArray[j];
          j++;
        }
        k++;
      }

      // 将还剩余元素没有遍历完的数组直接追加到intArr后面
      if (i == leftArray.length) {
        for (; j < rightArray.length; j++, k++) {
          intArr[k] = rightArray[j];
        }
      } else {
        for (; i < leftArray.length; i++, k++) {
          intArr[k] = leftArray[i];
        }
      }
    }
  }

  public static void main(String[] args) {

    // 变量定义
    long begintime = 0;
    long endtime = 0;

    // 生成排序数据
    int[] rawArr = generateIntArray(10000000);
    int[] rawArr2 = Arrays.copyOf(rawArr, rawArr.length);

    begintime = new Date().getTime();
    new SingleThreadMergeSort().sort(rawArr);
    endtime = new Date().getTime();
    System.out.println("单线程归并排序花费时间：" + (endtime - begintime));

    for (int i = 1; i <= 20; i++) {
      begintime = new Date().getTime();
      new ForkJoinMergeSort().sort(rawArr2, i);
      endtime = new Date().getTime();
      System.out.println("Fork/Join归并排序花费时间：" + "," + i + "," + (endtime - begintime));
    }

  }

  /**
   * 生成int类型的数组
   */
  private static int[] generateIntArray(int length) {
    int[] intArr = new int[length];
    for (int i = 0; i < length; i++) {
      intArr[i] = new Double(Math.random() * length).intValue();
    }
    return intArr;
  }
}
