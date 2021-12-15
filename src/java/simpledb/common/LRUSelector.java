package simpledb.common;


import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.HashMap;

public class LRUSelector<T> {

    private class Node<T> {
        private T t;
        private Node pre, next;

        public Node(){
            t = null;
            pre = null;
            next = null;
        }

        public Node(T t){
            this.t = t;
            pre = null;
            next = null;
        }

    }

    private Node<T> head;
    private Node<T> tail;
    private Integer capacity;
    private HashMap<T, Node<T>> lruMap;

    public LRUSelector(Integer capacity) {
        head = new Node<T>();
        tail = new Node<T>();
        head.next = tail;
        tail.pre = head;
        this.capacity = capacity;
        lruMap = new HashMap<>();
    }

    private void addToHead(T t) {
        Node<T> node = new Node<T>(t);
        node.pre = head;
        node.next = head.next;
        head.next.pre = node;
        head.next = node;
        lruMap.put(t, node);
    }

    private void removeNode(Node node) {
        if (node == null || lruMap.get(node) == null) {
            return;
        }
        node.next.pre = node.pre;
        node.pre.next = node.next;
        node.pre = null;
        node.next = null;
        lruMap.remove(node.t);
    }

    public T markUsedNow(T t){
        Node node = lruMap.get(t);
        if (node == null) {
            if (lruMap.size() < capacity) {
                addToHead(t);
                return null;
            } else {
             return expireOne();
            }
        }
        removeNode(node);
        addToHead(t);
        return null;
    }

    public Boolean isExist(T t) {
        return t != null && lruMap.get(t) != null;
    }

    private T expireOne() {
        if (lruMap.size() == 0) {
            return null;
        }
        else {
            Node<T> node = tail.pre;
            removeNode(node);
            return node.t;
        }
    }

}
