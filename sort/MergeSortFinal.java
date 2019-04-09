package com.ghgj.cn.sort;
/*
 * 1.拆：将一个数组的每一个元素拆成一个小的数据集
 * 	递归
 * 2.将上一步的拆的每一个小数据集  进行归并排序
 * 	递归
 */

import java.util.Arrays;

public class MergeSortFinal {
	public static void main(String[] args) {
		int[] arr={2,1,34,5,23,14,56};
		chai(arr, 0, arr.length-1);
		System.out.println(Arrays.toString(arr));
	}
	
	//1.写一个普通方法
	/**
	 * 
	 * @param arr   需要排序的数组
	 * @param left  每一个拆分完成之后的小数据集的起始下标
	 * @param right  每一个拆分完成之后的小数据集的结束下标
	 */
	public static void chai(int[] arr,int left,int right){
		//2.出口  left==right
		if(left>=right){
			return;
		}else{//需要拆分
			//递归拆分
			//求中间的下标
			int mid=(left+right)/2;
			//按照mid为边界进行拆分
			//左半部分
			chai(arr, left, mid);
			//右半部分
			chai(arr,mid+1,right);
			//递归排序了
			mergeSort(arr,left,right,mid);
			
		}
		
		
	}
	/*
	 * 归并每一个小数据集的过程
	 */
	public static void mergeSort(int[] arr, int left, int right, int mid) {
		/*
		 * [1, 2, 0, 0, 0, 0, 0]  resarr
			[0, 0, 5, 34, 0, 0, 0]  resarr
			
			1,2,5,34  resarr
			2,1--34,5   arr
			2,1,34,5
		 */
		//归并的部分  left---right之间的
		//被mid分成2部分 arr1---> left---mid        arr2--->mid+1---right
		//定义一个存放结果数据集的数组
		int[] resarr=new int[arr.length];
		
		//定义每一个数据集的起始下标
		int m=left;  //左侧的起始下标  m=0
		int n=mid+1;  //右侧数据集的起始下标  n=1
		
		//定义每一个小数据集的结束下标
		int x=mid;  //左侧数据集的结束下标  0
		int y=right;  //右侧数据集的结束下标   1
		
		//定义最终结果集的下标
		int index=left;  //0
		
		//开始并的过程
		//arr[m]=2   arr[n]=1
		while(m<=x && n<=y){
			if(arr[m]<=arr[n]){
				//将原始数据集赋值给resarr的
				resarr[index++]=arr[m++];
			}else{
				//resarr【0】=1
				resarr[index++]=arr[n++];
			}
		}
		
		//有可能  一个数据集还没有到头
		while(m<=x){
			resarr[index++]=arr[m++];
		}
		
		while(n<=y){
			resarr[index++]=arr[n++];
		}
		
		//每一次归并排序完成之后  应该把结果放在原始数组中  resarr   left-right   对应数组的   left--right
		for(int i=left;i<=right;i++){
			arr[i]=resarr[i];
		}
		//System.out.println(Arrays.toString(resarr));
		
	}

}
