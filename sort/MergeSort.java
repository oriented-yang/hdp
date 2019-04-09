package com.ghgj.cn.sort;

import java.util.Arrays;

public class MergeSort {
	public static void main(String[] args) {
		int[] arr1={1,2,3,5,6,8,10};
		int[] arr2={2,3,4,7,9,10,20,21,23,24};
		System.out.println(Arrays.toString(msort(arr1, arr2)));
	}
	//定义一个归并排序的方法
	public static int[] msort(int[] arr1,int[] arr2){
		//定义一个结果集
		int[] resarr=new int[arr1.length+arr2.length];
		//定义3个变量  分别记录3个数组的下标变化的
		int m=0;//arr1下标
		int n=0;//arr2下标
		int index=0;//resarr下标的
		//循环条件  只要两个数组中都有元素  就得比较
		//arr1有元素   m<=arr1.length-1
		//arr2有元素   n<=arr2.length-1  while|for
		while( m<=arr1.length-1 && n<=arr2.length-1){
			//比较  小的进最终数组
			if(arr1[m]<= arr2[n]){
				//小的给最终的新的数组赋值
				resarr[index++]=arr1[m++];
				/*index++;
				m++;*/
			}else{//arr1[m]> arr2[n]
				resarr[index++]=arr2[n++];
			}
		}
		
		//m<=arr1.length-1 && n<=arr2.length-1  有一个条件不满足  至少有一个数组已经没有元素了
		//假设arr1   m
		while(m<=arr1.length-1){
			resarr[index++]=arr1[m++];
		}
		//可能arr2
		while(n<=arr2.length-1){
			resarr[index++]=arr2[n++];
		}
		return resarr;
	}

}
