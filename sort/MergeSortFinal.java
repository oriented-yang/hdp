package com.ghgj.cn.sort;
/*
 * 1.�𣺽�һ�������ÿһ��Ԫ�ز��һ��С�����ݼ�
 * 	�ݹ�
 * 2.����һ���Ĳ��ÿһ��С���ݼ�  ���й鲢����
 * 	�ݹ�
 */

import java.util.Arrays;

public class MergeSortFinal {
	public static void main(String[] args) {
		int[] arr={2,1,34,5,23,14,56};
		chai(arr, 0, arr.length-1);
		System.out.println(Arrays.toString(arr));
	}
	
	//1.дһ����ͨ����
	/**
	 * 
	 * @param arr   ��Ҫ���������
	 * @param left  ÿһ��������֮���С���ݼ�����ʼ�±�
	 * @param right  ÿһ��������֮���С���ݼ��Ľ����±�
	 */
	public static void chai(int[] arr,int left,int right){
		//2.����  left==right
		if(left>=right){
			return;
		}else{//��Ҫ���
			//�ݹ���
			//���м���±�
			int mid=(left+right)/2;
			//����midΪ�߽���в��
			//��벿��
			chai(arr, left, mid);
			//�Ұ벿��
			chai(arr,mid+1,right);
			//�ݹ�������
			mergeSort(arr,left,right,mid);
			
		}
		
		
	}
	/*
	 * �鲢ÿһ��С���ݼ��Ĺ���
	 */
	public static void mergeSort(int[] arr, int left, int right, int mid) {
		/*
		 * [1, 2, 0, 0, 0, 0, 0]  resarr
			[0, 0, 5, 34, 0, 0, 0]  resarr
			
			1,2,5,34  resarr
			2,1--34,5   arr
			2,1,34,5
		 */
		//�鲢�Ĳ���  left---right֮���
		//��mid�ֳ�2���� arr1---> left---mid        arr2--->mid+1---right
		//����һ����Ž�����ݼ�������
		int[] resarr=new int[arr.length];
		
		//����ÿһ�����ݼ�����ʼ�±�
		int m=left;  //������ʼ�±�  m=0
		int n=mid+1;  //�Ҳ����ݼ�����ʼ�±�  n=1
		
		//����ÿһ��С���ݼ��Ľ����±�
		int x=mid;  //������ݼ��Ľ����±�  0
		int y=right;  //�Ҳ����ݼ��Ľ����±�   1
		
		//�������ս�������±�
		int index=left;  //0
		
		//��ʼ���Ĺ���
		//arr[m]=2   arr[n]=1
		while(m<=x && n<=y){
			if(arr[m]<=arr[n]){
				//��ԭʼ���ݼ���ֵ��resarr��
				resarr[index++]=arr[m++];
			}else{
				//resarr��0��=1
				resarr[index++]=arr[n++];
			}
		}
		
		//�п���  һ�����ݼ���û�е�ͷ
		while(m<=x){
			resarr[index++]=arr[m++];
		}
		
		while(n<=y){
			resarr[index++]=arr[n++];
		}
		
		//ÿһ�ι鲢�������֮��  Ӧ�ðѽ������ԭʼ������  resarr   left-right   ��Ӧ�����   left--right
		for(int i=left;i<=right;i++){
			arr[i]=resarr[i];
		}
		//System.out.println(Arrays.toString(resarr));
		
	}

}
