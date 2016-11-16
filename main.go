//
// main.go
// Copyright (C) 2016 yanming02 <yanming02@baidu.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	SourceAddr  = flag.String("sourceaddr", "", "source zk cluster address")
	TargetAddr  = flag.String("targetaddr", "", "target zk cluster address")
	ExcludePath = flag.String("excludepath", "/r3/failover/history,/r3/failover/doing", "exclude path")
)

const PERM_FILE = zk.PermAdmin | zk.PermRead | zk.PermWrite
const PERM_DIRECTORY = zk.PermAdmin | zk.PermCreate | zk.PermDelete | zk.PermRead | zk.PermWrite

func resolveIPv4Addr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	ipAddrs, err := net.LookupIP(host)
	for _, ipAddr := range ipAddrs {
		ipv4 := ipAddr.To4()
		if ipv4 != nil {
			return net.JoinHostPort(ipv4.String(), port), nil
		}
	}
	return "", fmt.Errorf("no IPv4addr for name %v", host)
}

func resolveZkAddr(zkAddr string) ([]string, error) {
	parts := strings.Split(zkAddr, ",")
	resolved := make([]string, 0, len(parts))
	for _, part := range parts {
		// The zookeeper client cannot handle IPv6 addresses before version 3.4.x.
		if r, err := resolveIPv4Addr(part); err != nil {
			log.Println("cannot resolve %v, will not use it: %v", part, err)
		} else {
			resolved = append(resolved, r)
		}
	}
	if len(resolved) == 0 {
		return nil, fmt.Errorf("no valid address found in %v", zkAddr)
	}
	return resolved, nil
}

func DialZk(zkAddr string) (*zk.Conn, <-chan zk.Event, error) {
	resolvedZkAddr, err := resolveZkAddr(zkAddr)
	if err != nil {
		return nil, nil, err
	}

	zconn, session, err := zk.Connect(resolvedZkAddr, 5e9)
	if err == nil {
		// Wait for connection, possibly forever
		event := <-session
		if event.State != zk.StateConnected && event.State != zk.StateConnecting {
			err = fmt.Errorf("zk connect failed: %v", event.State)
		}
		if err == nil {
			return zconn, session, nil
		} else {
			zconn.Close()
		}
	}
	return nil, nil, err
}

func walk(root string, sconn *zk.Conn, tconn *zk.Conn, excludePaths []string) {

	children, _, err := sconn.Children(root)
	if err != nil {
		log.Printf("error, when get children of %s, %s\n", root, err)
		os.Exit(1)
	}

	for _, node := range children {
		fullpath := path.Join(root, node)
		if IsPathExcluded(excludePaths, fullpath) {
			return
		}
		data, stat, _ := sconn.Get(fullpath)
		if stat.EphemeralOwner == 0 {
			// ignore ephemeral node
			_, err := CreateRecursive(tconn, fullpath, data, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				log.Printf("error, when create node in target zk, %v\n", err)
				os.Exit(1)
			}
			log.Println(fullpath, " backup success")
		}
		walk(fullpath, sconn, tconn, excludePaths)
	}
}

func CreateRecursive(zconn *zk.Conn, zkPath string, value []byte, flags int32, aclv []zk.ACL) (pathCreated string, err error) {
	exists, _, err := zconn.Exists(zkPath)
	if err != nil {
		return "", err
	}
	if exists {
		zconn.Delete(zkPath, -1)
	}
	pathCreated, err = zconn.Create(zkPath, value, flags, aclv)
	if err == zk.ErrNoNode {
		dirAclv := make([]zk.ACL, len(aclv))
		for i, acl := range aclv {
			dirAclv[i] = acl
			dirAclv[i].Perms = PERM_DIRECTORY
		}
		_, err = CreateRecursive(zconn, path.Dir(zkPath), []byte(""), flags, dirAclv)
		if err != nil && err != zk.ErrNodeExists {
			return "", err
		}
		pathCreated, err = zconn.Create(zkPath, value, flags, aclv)
	}
	return pathCreated, nil
}

func IsPathExcluded(paths []string, path string) bool {
	for _, p := range paths {
		if p == path {
			return true
		}
	}
	return false
}

func main() {
	flag.Parse()

	if *SourceAddr == "" || *TargetAddr == "" {
		log.Println("Args should be assigned")
		os.Exit(1)
	}
	exPaths := []string{}
	if *ExcludePath != "" {
		exPaths = strings.Split(*ExcludePath, ",")
	}

	sourceConn, _, err := DialZk(*SourceAddr)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	targetConn, _, err := DialZk(*TargetAddr)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	walk("/r3", sourceConn, targetConn, exPaths)

}
