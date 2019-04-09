package com.ghgj.cn.sort;

import java.util.Arrays;

public class MergeSort {
	public static void main(String[] args) {
		int[] arr1={1,2,3,5,6,8,10};
		int[] arr2={2,3,4,7,9,10,20,21,23,24};
		System.out.println(Arrays.toString(msort(arr1, arr2)));
	}
	//����һ���鲢����ķ���
	public static int[] msort(int[] arr1,int[] arr2){
		//����һ�������
		int[] resarr=new int[arr1.length+arr2.length];
		//����3������  �ֱ��¼3��������±�仯��
		int m=0;//arr1�±�
		int n=0;//arr2�±�
		int index=0;//resarr�±��
		//ѭ������  ֻҪ���������ж���Ԫ��  �͵ñȽ�
		//arr1��Ԫ��   m<=arr1.length-1
		//arr2��Ԫ��   n<=arr2.length-1  while|for
		while( m<=arr1.length-1 && n<=arr2.length-1){
			//�Ƚ�  С�Ľ���������
			if(arr1[m]<= arr2[n]){
				//С�ĸ����յ��µ����鸳ֵ
				resarr[index++]=arr1[m++];
				/*index++;
				m++;*/
			}else{//arr1[m]> arr2[n]
				resarr[index++]=arr2[n++];
			}
		}
		
		//m<=arr1.length-1 && n<=arr2.length-1  ��һ������������  ������һ�������Ѿ�û��Ԫ����
		//����arr1   m
		while(m<=arr1.length-1){
			resarr[index++]=arr1[m++];
		}
		//����arr2
		while(n<=arr2.length-1){
			resarr[index++]=arr2[n++];
		}
		return resarr;
	}

}
