const awsConfig = {
    Auth: {
      Cognito: {
        userPoolId: "ap-southeast-2_bjbASTsi3",       
        userPoolClientId: "3o76e6atudmuc6t82tq8i3fp25", 
        loginWith: {
          oauth: {
            domain: "ap-southeast-2bjbastsi3.auth.ap-southeast-2.amazoncognito.com",
            scopes: ["openid", "email", "profile"],
            redirectSignIn: ["https://jay-yoon10.github.io/canva-asset-hub"],
            redirectSignOut: ["https://jay-yoon10.github.io/canva-asset-hub"],
            responseType: "code", // PKCE trigger
          },
        },
      },
    },
  };
  
  export default awsConfig;