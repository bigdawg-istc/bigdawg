var one_tick = 20; //seconds

//service name must be unique for a certain host.
//host name must be unique

module.exports =
  [
    {
      name:'Apple HTTPS',
      host: 'www.apple.com',
      port:443,
      protocol: 'https',
      ping_service_name: 'http',
      timeout:10000,
      ping_interval: one_tick, //seconds
      failed_ping_interval: one_tick / 3, //minutes
      enabled: true,
      alert_to: ['ivan@iloire.com'],
      warning_if_takes_more_than: 1500, //miliseconds
      services : [
        {
          name: 'home',
          method: 'get',
          url : '/',
          expected: {statuscode: 200, contains: 'Apple Inc'}
        }
      ]
    }
  ]
