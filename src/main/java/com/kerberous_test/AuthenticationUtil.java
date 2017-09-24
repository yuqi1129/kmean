package com.kerberous_test;

import com.google.common.base.Preconditions;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: clp
 * Date: 13-8-12
 * Time: 下午5:36
 * To change this template use File | Settings | File Templates.
 */
public class AuthenticationUtil {
    static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(AuthenticationUtil.class);

    private static UserGroupInformation proxyTicket;

    private static final AtomicReference<KerberosUser> staticLogin = new AtomicReference<KerberosUser>();



    public static boolean authenticate(String kerbConfPrincipal,String kerbKeytab,String proxyUserName) {

        // logic for kerberos login
        boolean useSecurity = UserGroupInformation.isSecurityEnabled();

        LOG.info("Hadoop Security enabled: " + useSecurity);

        if (useSecurity) {

            // sanity checking
            if (kerbConfPrincipal.isEmpty()) {
                LOG.error("Hadoop running in secure mode, but Flume config doesn't "
                        + "specify a principal to use for Kerberos auth.");
                return false;
            }
            if (kerbKeytab.isEmpty()) {
                LOG.error("Hadoop running in secure mode, but Flume config doesn't "
                        + "specify a keytab to use for Kerberos auth.");
                return false;
            }

            String principal;
            try {
                // resolves _HOST pattern using standard Hadoop search/replace
                // via DNS lookup when 2nd argument is empty
                principal = SecurityUtil.getServerPrincipal(kerbConfPrincipal, "");
            } catch (IOException e) {
                LOG.error("Host lookup error resolving kerberos principal ("
                        + kerbConfPrincipal + "). Exception follows.", e);
                return false;
            }

            Preconditions.checkNotNull(principal, "Principal must not be null");
            KerberosUser prevUser = staticLogin.get();
            KerberosUser newUser = new KerberosUser(principal, kerbKeytab);

            // be cruel and unusual when user tries to login as multiple principals
            // this isn't really valid with a reconfigure but this should be rare
            // enough to warrant a restart of the agent JVM
            // TODO: find a way to interrogate the entire current config state,
            // since we don't have to be unnecessarily protective if they switch all
            // HDFS sinks to use a different principal all at once.
            Preconditions.checkState(prevUser == null || prevUser.equals(newUser),
                    "Cannot use multiple kerberos principals in the same agent. " +
                            " Must restart agent to use new principal or keytab. " +
                            "Previous = %s, New = %s", prevUser, newUser);

            // attempt to use cached credential if the user is the same
            // this is polite and should avoid flooding the KDC with auth requests
            UserGroupInformation curUser = null;
            if (prevUser != null && prevUser.equals(newUser)) {
                try {
                    curUser = UserGroupInformation.getLoginUser();
                } catch (IOException e) {
                    LOG.warn("User unexpectedly had no active login. Continuing with " +
                            "authentication", e);
                }
            }

            if (curUser == null || !curUser.getUserName().equals(principal)) {
                try {
                    // static login
                    kerberosLogin(principal, kerbKeytab);
                } catch (IOException e) {
                    LOG.error("Authentication or file read error while attempting to "
                            + "login as kerberos principal (" + principal + ") using "
                            + "keytab (" + kerbKeytab + "). Exception follows.", e);
                    return false;
                }
            } else {
                LOG.debug("{}: Using existing principal login: {}", "", curUser);
            }

            // we supposedly got through this unscathed... so store the static user
            staticLogin.set(newUser);
        }

        // hadoop impersonation works with or without kerberos security
        proxyTicket = null;
        if (!proxyUserName.isEmpty()) {
            try {
                proxyTicket = UserGroupInformation.createProxyUser(
                        proxyUserName, UserGroupInformation.getLoginUser());
            } catch (IOException e) {
                LOG.error("Unable to login as proxy user. Exception follows.", e);
                return false;
            }
        }

        UserGroupInformation ugi = null;
        if (proxyTicket != null) {
            ugi = proxyTicket;
        } else if (useSecurity) {
            try {
                ugi = UserGroupInformation.getLoginUser();
            } catch (IOException e) {
                LOG.error("Unexpected error: Unable to get authenticated user after " +
                        "apparent successful login! Exception follows.", e);
                return false;
            }
        }

        if (ugi != null) {
            // dump login information
            UserGroupInformation.AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
            LOG.info("Auth method: {}", authMethod);
            LOG.info(" User name: {}", ugi.getUserName());
            LOG.info(" Using keytab: {}", ugi.isFromKeytab());
            if (authMethod == UserGroupInformation.AuthenticationMethod.PROXY) {
                UserGroupInformation superUser;
                try {
                    superUser = UserGroupInformation.getLoginUser();
                    LOG.info(" Superuser auth: {}", superUser.getAuthenticationMethod());
                    LOG.info(" Superuser name: {}", superUser.getUserName());
                    LOG.info(" Superuser using keytab: {}", superUser.isFromKeytab());
                } catch (IOException e) {
                    LOG.error("Unexpected error: unknown superuser impersonating proxy.",
                            e);
                    return false;
                }
            }

            LOG.info("Logged in as user {}", ugi.getUserName());

            return true;
        }

        return true;
    }

    /**
     * Static synchronized method for static Kerberos login. <br/>
     * Static synchronized due to a thundering herd problem when multiple Sinks
     * attempt to log in using the same principal at the same time with the
     * intention of impersonating different users (or even the same user).
     * If this is not controlled, MIT Kerberos v5 believes it is seeing a replay
     * attach and it returns:
     * <blockquote>Request is a replay (34) - PROCESS_TGS</blockquote>
     * In addition, since the underlying Hadoop APIs we are using for
     * impersonation are static, we define this method as static as well.
     *
     * @param principal Fully-qualified principal to use for authentication.
     * @param keytab Location of keytab file containing credentials for principal.
     * @return Logged-in user
     * @throws IOException if login fails.
     */
    private static synchronized UserGroupInformation kerberosLogin(String principal, String keytab) throws IOException {

        // if we are the 2nd user thru the lock, the login should already be
        // available statically if login was successful
        UserGroupInformation curUser = null;
        try {
            curUser = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            // not a big deal but this shouldn't typically happen because it will
            // generally fall back to the UNIX user
            LOG.debug("Unable to get login user before Kerberos auth attempt.", e);
        }

        // we already have logged in successfully
        if (curUser != null && curUser.getUserName().equals(principal)) {
            LOG.debug("{}: Using existing principal ({}): {}",
                    new Object[] { "", principal, curUser });

            // no principal found
        } else {

            LOG.info("{}: Attempting kerberos login as principal ({}) from keytab " +
                    "file ({})", new Object[] { "", principal, keytab });

            // attempt static kerberos login
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            curUser = UserGroupInformation.getLoginUser();
        }

        return curUser;
    }

}
