package com.edu.hust.word.similarity.hownet.concept;

import java.util.LinkedList;

/**
 * 概念处理列
 * @author xuming
 */
public class ConceptLinkedList extends LinkedList<Concept>{
    private static final long serialVersionUID = -4505203846911972621L;

    public void removeLast(int size){
        for(int i =0;i<size;i++){
            removeLast();
        }
    }

    public void addByDefine(Concept concept){
        for(Concept c:this){
            if(c.getDefine().equals(concept.getDefine())){
                return;
            }
        }
        add(concept);
    }
}
