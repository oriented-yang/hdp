package com.ghgj.cn.sort;

import java.util.Arrays;

public class QuickSort {
	
	
	public static void main(String[] args) {
		int[] arr={23,12,34,10,40,3};
		System.out.println(Arrays.toString(arr));
		qsort(arr, 0, arr.length-1);
		System.out.println(Arrays.toString(arr));
	}
	
	
	//��������
	//1.����һ����ͨ����
	public static void qsort(int[] arr,int left,int right){
		//2.�ҳ���
		if(left>=right){
			return;
		}else{
			//���￪ʼ���еݹ�����
			//��ȡ��һ���ֽ��
			int index=getFinalIndex(arr, left, right);
			//�ֽ�����
			qsort(arr, left, index-1);
			//�ֽ���Ҳ�
			qsort(arr, index+1, right);
		}
	}
	//��ȡÿһ�ֵ����յķֽ��   �±�
	//������ԭʼ����
	//����ֵ    ���յ��±�
	/*
	 * [3, 12, 10 ,k,  40, 34]
	 */
	public static int getFinalIndex(int[] arr,int left,int right){
		//1.��ȡ�߽�  key=2
		int key=arr[left];
		//2   3    1	5    8
		//0	  1    2    3    4
		while(left<right){//������   ���ѭ��һ��
			//3,12,10,k,40,34
			//��������  ѭ������  �����ÿһ��Ԫ��  ��key�Ƚ�
			while(arr[right]>=key && left<right){//�ڲ����е�
				right--;
			}
			//arr[2]<key    arr[right]  key  arr[0]
			arr[left]=arr[right];
			//arr[right]=key;
			//1   3    2k	5    8
			//��������   ѭ������ÿһ��Ԫ��
			while(arr[left]<=key && left<right){
				left++;
			}
			//arr[left]>key  arr[left]  key  arr[right]
			arr[right]=arr[left];
			//arr[left]=key;
			//1   3    2k	5    8
			//1   2k   3    5    8
		}
		//λ��  left   right
		arr[left]=key;
		//left==right
		return left;
		
		
	}

}
