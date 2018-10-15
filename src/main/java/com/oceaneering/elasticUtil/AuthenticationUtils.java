/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oceaneering.elasticUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.oceaneering.exceptions.TokenAuthenticationException;
import com.oceaneering.properties.WrapperProperties;
import com.oceaneering.search.EsRequest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.MimeHeaders;
import javax.xml.soap.Node;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import org.w3c.dom.NodeList;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;

/**
 *
 * @author SShukla
 *
 */
public class AuthenticationUtils {

    public static Map<String, String> getUserDetails(String uname) {
        Map<String, String> userInfo = null;
        try {
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            String url = "https://appdevdev.oii.oceaneering.com/MDS/security.asmx";
            SOAPMessage soapResponse = soapConnection.call(createUserInfoSOAPRequest(uname), url);
            soapResponse.writeTo(System.out);
            userInfo = new HashMap<String, String>();
            userInfo = parseSoapResponse(soapResponse);
        } catch (Exception e) {
        }
        return userInfo;
    }

    private static SOAPMessage createUserInfoSOAPRequest(String uname) throws SOAPException, IOException {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();
        SOAPPart soapPart = soapMessage.getSOAPPart();

        String serverURI = "http://oceaneering.com/";
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration("example", serverURI);
        SOAPBody soapBody = envelope.getBody();
        SOAPElement soapBodyElem1 = soapBody
                .addBodyElement(envelope.createName("GetUserInfo", "", "http://oceaneering.com/"));
        soapBodyElem1.addChildElement("userID").addTextNode(uname);

        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", "http://oceaneering.com/" + "GetUserInfo");
        soapMessage.writeTo(System.out);
        return soapMessage;
    }

    private static Map<String, String> parseSoapResponse(SOAPMessage soapResponse) throws SOAPException {
        String authorized = "false";
        Map<String, String> userInfo = null;
        if (soapResponse.getSOAPBody().getFirstChild().getFirstChild() == null) {
            userInfo = new HashMap<String, String>();
            userInfo.put("authorize", authorized);
            return userInfo;
        }
        Node sb = (Node) soapResponse.getSOAPBody().getChildElements().next();
        Node user = (Node) sb.getFirstChild().getFirstChild().getNextSibling().getFirstChild().getFirstChild();
        if (user != null) {
            authorized = "true";
            userInfo = new HashMap<String, String>();
            userInfo.put("authorize", authorized);
            String login = user.getFirstChild().getTextContent();
            userInfo.put("username", login);
            String empId = user.getFirstChild().getNextSibling().getFirstChild().getTextContent();
            userInfo.put("empId", empId);
        }
        return userInfo;
    }

    public static List<String> getUserRole(String appName, String username) {
        List<String> assignedRoles = null;
        try {
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            String url = "https://appdevdev.oii.oceaneering.com/MDS/security.asmx";
            SOAPMessage soapResponse = soapConnection.call(createAppRoleSOAPRequest(appName, username), url);
            assignedRoles = parseAppRoleSoapResponse(soapResponse);
        } catch (Exception e) {
            assignedRoles = new ArrayList<String>();
            return assignedRoles;
        }
        return assignedRoles;
    }

    private static SOAPMessage createAppRoleSOAPRequest(String appName, String username) throws SOAPException, IOException {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();
        SOAPPart soapPart = soapMessage.getSOAPPart();

        String serverURI = "http://oceaneering.com/";
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration("example", serverURI);
        SOAPBody soapBody = envelope.getBody();
        SOAPElement soapBodyElem1 = soapBody
                .addBodyElement(envelope.createName("GetAppRoles", "", "http://oceaneering.com/"));
        soapBodyElem1.addChildElement("app").addTextNode(appName);
        soapBodyElem1.addChildElement("login").addTextNode(username);
        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", "http://oceaneering.com/" + "GetAppRoles");
        return soapMessage;
    }

    private static List<String> parseAppRoleSoapResponse(SOAPMessage soapResponse) throws SOAPException {
        SOAPBody sb = soapResponse.getSOAPBody();
        NodeList nodeList = sb.getFirstChild().getFirstChild().getFirstChild().getChildNodes();
        int length = nodeList.getLength();
        List<String> roles = new ArrayList();
        int i = 0;
        while (i < length) {
            Node node = (Node) nodeList.item(i);
            String roleName = node.getAttributes().getNamedItem("name").getNodeValue();
            roles.add(roleName);
            i++;
        }
        return roles;
    }

    public static Map<String, String> validateToken(String token, String key) throws TokenAuthenticationException, IOException {
        String validated = "false";
        Map<String, String> body = null;
        if (token != null && key != null) {
            try {
                if (isTokenExists(token)) {
                    Jws<Claims> jwtClaims = Jwts.parser().setSigningKey(key.getBytes("UTF-8")).parseClaimsJws(token);
                    body = new HashMap(jwtClaims.getBody());
                } else {
                    throw new TokenAuthenticationException(114);
//                    body = new HashMap();
//                    validated = "false";
//                    body.put("validated", validated);

                }
            } catch (ExpiredJwtException e) {
                deleteExpiredTokens(token);
                throw new TokenAuthenticationException(114);
            } catch (SignatureException | MalformedJwtException e) {
                throw new TokenAuthenticationException(115);
            } catch (UnsupportedJwtException | IllegalArgumentException | UnsupportedEncodingException e) {
                throw new TokenAuthenticationException(116);
            }
        }
        return body;
    }

    public static boolean isTokenExists(String token) throws IOException {
        WrapperProperties jwtProps = WrapperProperties.Instance("JWT Expiry");
        JsonNode tokenStats = (new EsRequest(token, jwtProps))
                .setEndPoint(jwtProps.getESType().asText() + "/" + token)
                .get();
        if (!tokenStats.toString().contains("Not Found")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean deleteExpiredTokens(String expToken) throws IOException {
        WrapperProperties jwtProps = WrapperProperties.Instance("JWT Expiry");
        System.out.println(jwtProps.getESType().asText() + "/" + expToken);
        JsonNode tokenStats = (new EsRequest(expToken, jwtProps))
                .setEndPoint(jwtProps.getESType().asText() + "/" + expToken)
                .delete();
        if (tokenStats.toString().contains("deleted")) {
            return true;
        } else {
            return false;
        }
    }
}
