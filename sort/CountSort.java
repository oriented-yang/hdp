package com.ghgj.cn.sort;

public class CountSort {
	

	
	public static void main(String[] args) {
		int[] arr={2,3,4,6,23,1,4,2,4,6};
		count(arr);
	}
	
	//����   ԭʼ����
	public static void count(int[] arr){
		//����һ��������   ����  = ԭʼ��������ֵ+1
		//��ԭʼ��������ֵ
		int max=arr[0];
		for(int a:arr){
			if(max<a){
				max=a;
			}
		}
		
		//�����µ�������   ���ֵ-��Сֵ+1
		int[] newarr=new int[max+1];
		//ѭ������ԭʼ����   ��ԭʼ�����ֵ  ������������±���
		for(int a:arr){
			newarr[a]++;
		}
		//�Ѿ�������
		//ѭ���������
		for(int i=0;i<newarr.length;i++){
			//ѭ���������ֵĴ���
			/*if(newarr[i]>0){*/
			for(int j=0;j<newarr[i];j++){
				System.out.print(i+"\t");
			}
				
			//}
		}
		
	}
}
