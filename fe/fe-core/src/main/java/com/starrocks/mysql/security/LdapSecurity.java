// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mysql.security;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class LdapSecurity {
    private static final Logger LOG = LogManager.getLogger(LdapSecurity.class);

    //bind to ldap server to check password
    public static boolean checkPassword(String dn, String password) {
        if (Strings.isNullOrEmpty(password)) {
            LOG.warn("empty password is not allowed for simple authentication");
            return false;
        }

        String url = "ldap://" + Config.authentication_ldap_simple_server_host + ":" +
                Config.authentication_ldap_simple_server_port;
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, password);
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);

        DirContext ctx = null;
        try {
            //this will send a bind call to ldap server, throw exception if failed
            ctx = new InitialDirContext(env);
            return true;
        } catch (Exception e) {
            LOG.warn("check ldap password failed, dn = {}, password = {}", dn, password, e);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception e) {
                }
            }
        }

        return false;
    }

    //1. bind ldap server by root dn
    //2. search user
    //3. if match exactly one, check password
    public static boolean checkPasswordByRoot(String user, String password) {
        if (Strings.isNullOrEmpty(Config.authentication_ldap_simple_bind_root_pwd)) {
            LOG.warn("empty password is not allowed for simple authentication");
            return false;
        }

        String url = "ldap://" + Config.authentication_ldap_simple_server_host + ":" +
                Config.authentication_ldap_simple_server_port;
        Hashtable<String, String> env = new Hashtable<>();
        //dn contains '=', so we should use ' or " to wrap the value in config file
        String rootDN = Config.authentication_ldap_simple_bind_root_dn;
        rootDN = trim(rootDN, "\"");
        rootDN = trim(rootDN, "'");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, Config.authentication_ldap_simple_bind_root_pwd);
        env.put(Context.SECURITY_PRINCIPAL, rootDN);
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);

        DirContext ctx = null;
        try {
            String baseDN = Config.authentication_ldap_simple_bind_base_dn;
            baseDN = trim(baseDN, "\"");
            baseDN = trim(baseDN, "'");
            ctx = new InitialDirContext(env);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            String searchFilter = "(" + Config.authentication_ldap_simple_user_search_attr + "=" + user + ")";
            NamingEnumeration<SearchResult> results = ctx.search(baseDN, searchFilter, sc);

            String userDN = null;
            int matched = 0;
            for (; ; ) {
                if (results.hasMore()) {
                    matched++;
                    if (matched > 1) {
                        LOG.warn("searched more than one entry from ldap server for user {}", user);
                        return false;
                    }

                    SearchResult result = results.next();
                    userDN = result.getNameInNamespace();
                } else {
                    break;
                }
            }

            if (matched != 1) {
                LOG.warn("ldap search matched user count {}", matched);
                return false;
            }

            return checkPassword(userDN, password);
        } catch (Exception e) {
            LOG.warn("call ldap exception ", e);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception e) {
                }
            }
        }

        return false;
    }

    //trim prefix and suffix of target from src
    private static String trim(String src, String target) {
        if (src != null && target != null) {
            if (src.startsWith(target)) {
                src = src.substring(target.length());
            }
            if (src.endsWith(target)) {
                src = src.substring(0, src.length() - target.length());
            }
        }
        return src;
    }
}
