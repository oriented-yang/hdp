package com.ghgj.cn.sort;

import java.util.Arrays;

public class QuickSort {
	
	
	public static void main(String[] args) {
		int[] arr={23,12,34,10,40,3};
		System.out.println(Arrays.toString(arr));
		qsort(arr, 0, arr.length-1);
		System.out.println(Arrays.toString(arr));
	}
	
	
	//快速排序
	//1.定义一个普通方法
	public static void qsort(int[] arr,int left,int right){
		//2.找出口
		if(left>=right){
			return;
		}else{
			//这里开始进行递归排序
			//获取第一个分界点
			int index=getFinalIndex(arr, left, right);
			//分界点左侧
			qsort(arr, left, index-1);
			//分界点右侧
			qsort(arr, index+1, right);
		}
	}
	//获取每一轮的最终的分界点   下标
	//参数：原始数组
	//返回值    最终的下标
	/*
	 * [3, 12, 10 ,k,  40, 34]
	 */
	public static int getFinalIndex(int[] arr,int left,int right){
		//1.获取边界  key=2
		int key=arr[left];
		//2   3    1	5    8
		//0	  1    2    3    4
		while(left<right){//大条件   外层循环一次
			//3,12,10,k,40,34
			//从右向左  循环遍历  数组的每一个元素  和key比较
			while(arr[right]>=key && left<right){//内层所有的
				right--;
			}
			//arr[2]<key    arr[right]  key  arr[0]
			arr[left]=arr[right];
			//arr[right]=key;
			//1   3    2k	5    8
			//从左向右   循环遍历每一个元素
			while(arr[left]<=key && left<right){
				left++;
			}
			//arr[left]>key  arr[left]  key  arr[right]
			arr[right]=arr[left];
			//arr[left]=key;
			//1   3    2k	5    8
			//1   2k   3    5    8
		}
		//位置  left   right
		arr[left]=key;
		//left==right
		return left;
		
		
	}

}
