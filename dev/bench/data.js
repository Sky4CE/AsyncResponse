window.BENCHMARK_DATA = {
  "lastUpdate": 1782726386862,
  "repoUrl": "https://github.com/Sky4CE/AsyncResponse",
  "entries": {
    "AsyncResponse Microbenchmarks": [
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "336277848e28b1e7d6850920480786681c7f088c",
          "message": "added workflow yml to track performance",
          "timestamp": "2026-06-16T22:03:55+02:00",
          "tree_id": "e47c587b1108eb0a00844a032f8a52d7d22e63c2",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/336277848e28b1e7d6850920480786681c7f088c"
        },
        "date": 1781641092004,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 3318.665068308512,
            "unit": "ns",
            "range": "± 3.3964871972443396"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 3111.3492469787598,
            "unit": "ns",
            "range": "± 10.494580036526322"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 1440.808380762736,
            "unit": "ns",
            "range": "± 14.814523759864787"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 4884.801905314128,
            "unit": "ns",
            "range": "± 84.01146570549133"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 331.21032667160034,
            "unit": "ns",
            "range": "± 2.8007166322737724"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 607.2844092051188,
            "unit": "ns",
            "range": "± 3.5015622821735977"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 2.481557436287403,
            "unit": "ns",
            "range": "± 0.011597108951693772"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 4486.481313069661,
            "unit": "ns",
            "range": "± 35.14350859525638"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "22035f2a553c97a781d1704f559d5e08c2930f5b",
          "message": "performance fix",
          "timestamp": "2026-06-16T23:11:02+02:00",
          "tree_id": "f890deb570c7a8d0e6511f623cf7b288fcf08580",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/22035f2a553c97a781d1704f559d5e08c2930f5b"
        },
        "date": 1781644389478,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2850.4515635172525,
            "unit": "ns",
            "range": "± 4.155039877245168"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2614.9584426879883,
            "unit": "ns",
            "range": "± 9.729321503945384"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 1512.3001887003581,
            "unit": "ns",
            "range": "± 5.990720778122948"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 5041.34605662028,
            "unit": "ns",
            "range": "± 20.37969791696547"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 387.59727970759076,
            "unit": "ns",
            "range": "± 4.284801217591032"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 654.7595936457316,
            "unit": "ns",
            "range": "± 2.622325081039047"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 2.5450028106570244,
            "unit": "ns",
            "range": "± 0.04863716057478431"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 231.38724978764853,
            "unit": "ns",
            "range": "± 1.1488431948930447"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ffbb46cb8b4713afec451b9a02dc3de9585069dd",
          "message": "added NBomber load test",
          "timestamp": "2026-06-17T01:31:48+02:00",
          "tree_id": "ccb860d192b362baf5d11235c1ce3db34a0d2c39",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ffbb46cb8b4713afec451b9a02dc3de9585069dd"
        },
        "date": 1781652824222,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2803.516429901123,
            "unit": "ns",
            "range": "± 17.993101929923455"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2557.404355367025,
            "unit": "ns",
            "range": "± 5.992444915855777"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 1506.5249830881755,
            "unit": "ns",
            "range": "± 37.56045432100352"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 4930.385533650716,
            "unit": "ns",
            "range": "± 13.690875660900753"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 381.56820217768353,
            "unit": "ns",
            "range": "± 0.5550416609288593"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 636.7260360717773,
            "unit": "ns",
            "range": "± 0.8411958672348485"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 2.4974377527832985,
            "unit": "ns",
            "range": "± 0.00583829555599803"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 239.2580714225769,
            "unit": "ns",
            "range": "± 2.0453924548896762"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "153850349b435011c16eebed4de044bcc0ddaa7e",
          "message": "Refactored ClassifyOutcome in favor of ShouldResumeOnRecovery",
          "timestamp": "2026-06-17T16:57:01+02:00",
          "tree_id": "071b5f9af921a0da084c38210926e4a7e5b3f2b1",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/153850349b435011c16eebed4de044bcc0ddaa7e"
        },
        "date": 1781708363864,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2817.438886642456,
            "unit": "ns",
            "range": "± 11.653629499042278"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2531.6408818562827,
            "unit": "ns",
            "range": "± 2.7084976453838934"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 1531.0079085032146,
            "unit": "ns",
            "range": "± 14.787785593729202"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 5112.492197672526,
            "unit": "ns",
            "range": "± 40.009469592424885"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 395.5555033683777,
            "unit": "ns",
            "range": "± 0.887011403892247"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 663.8173961639404,
            "unit": "ns",
            "range": "± 3.036962416086941"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.547220514466365,
            "unit": "ns",
            "range": "± 0.00205782833618525"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 233.6362111568451,
            "unit": "ns",
            "range": "± 6.474477817021121"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "2f952d5b7b7f9263e434757c1b0da1d42fedca63",
          "message": "chore: cleanup/reanming",
          "timestamp": "2026-06-17T17:21:49+02:00",
          "tree_id": "2b738c83dddae1dd408f3e58fb597329bf65ea65",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/2f952d5b7b7f9263e434757c1b0da1d42fedca63"
        },
        "date": 1781709858066,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 3064.8917605082192,
            "unit": "ns",
            "range": "± 21.118992597014405"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2871.2074292500815,
            "unit": "ns",
            "range": "± 16.597271789494354"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 1468.900692621867,
            "unit": "ns",
            "range": "± 53.81259804145086"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 4904.36251449585,
            "unit": "ns",
            "range": "± 94.88039131597179"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 338.43551166852313,
            "unit": "ns",
            "range": "± 0.6750807656994605"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 610.5892314910889,
            "unit": "ns",
            "range": "± 6.852569812121542"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6768970216313999,
            "unit": "ns",
            "range": "± 0.004090075461438483"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 228.12771558761597,
            "unit": "ns",
            "range": "± 0.3979700313521574"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ebc07896c052d7f805ef223c02dfa22989cccdcd",
          "message": "Implemented Source-generated logging and extension",
          "timestamp": "2026-06-18T01:03:17+02:00",
          "tree_id": "c4d501288f636132f27750f885d5bea3642ddf68",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ebc07896c052d7f805ef223c02dfa22989cccdcd"
        },
        "date": 1781737514794,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2796.9309272766113,
            "unit": "ns",
            "range": "± 6.106790646195557"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2547.3387921651206,
            "unit": "ns",
            "range": "± 4.286608858808728"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 1517.9602216084797,
            "unit": "ns",
            "range": "± 9.032921609173139"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 4961.468933105469,
            "unit": "ns",
            "range": "± 12.212030176221667"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 408.6377444267273,
            "unit": "ns",
            "range": "± 2.6026869150657066"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 644.6417996088663,
            "unit": "ns",
            "range": "± 1.0174205968081413"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5520951797564824,
            "unit": "ns",
            "range": "± 0.0019555252786929342"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 236.9320559501648,
            "unit": "ns",
            "range": "± 1.6434114453376314"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c4f5bf04253150fa84ede2b59904e7c1c0f842d6",
          "message": "Optimized reflection invocation",
          "timestamp": "2026-06-18T01:43:17+02:00",
          "tree_id": "13edc6b568bd797ef2ecbbe5b0c2d47caf7159fc",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c4f5bf04253150fa84ede2b59904e7c1c0f842d6"
        },
        "date": 1781739905814,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2767.1322542826333,
            "unit": "ns",
            "range": "± 10.742448988978522"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2513.6115175882974,
            "unit": "ns",
            "range": "± 4.26331446975833"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 72.01097106933594,
            "unit": "ns",
            "range": "± 0.08269938979878134"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 69.03752015034358,
            "unit": "ns",
            "range": "± 0.257100184147866"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 383.34312804539996,
            "unit": "ns",
            "range": "± 0.6158515091900556"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 640.1697565714518,
            "unit": "ns",
            "range": "± 5.289467313673205"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 2.567077614367008,
            "unit": "ns",
            "range": "± 0.005415065976135454"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 246.29924154281616,
            "unit": "ns",
            "range": "± 0.8077590359305069"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1076187437db7250b4004ade902f9a29f3be0a3a",
          "message": "Implemented AsyncResponseDiagnostics",
          "timestamp": "2026-06-18T02:23:19+02:00",
          "tree_id": "74343527b6844a6ec95393f0eaf58e8f09e84d2e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1076187437db7250b4004ade902f9a29f3be0a3a"
        },
        "date": 1781742320143,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 3199.222873687744,
            "unit": "ns",
            "range": "± 26.474365179559275"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2930.852308909098,
            "unit": "ns",
            "range": "± 1.1798707323453486"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 84.1066624323527,
            "unit": "ns",
            "range": "± 0.7914142165996724"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 60.16807589928309,
            "unit": "ns",
            "range": "± 0.37496946788305036"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 342.9159887631734,
            "unit": "ns",
            "range": "± 0.7282787300856164"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 604.057624499003,
            "unit": "ns",
            "range": "± 4.348812637591549"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6777138424416382,
            "unit": "ns",
            "range": "± 0.0024599171226677165"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 235.92462619145712,
            "unit": "ns",
            "range": "± 1.0363731514540488"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "d5b3851d0d74c8482c267762e70caf7f58f4f293",
          "message": "IAsyncResponsePublisher.SetResponse<T> is now constrained to where T : IAsyncResponsePayload",
          "timestamp": "2026-06-18T10:03:56+02:00",
          "tree_id": "50180d8491650ae9fa26b68bcc8b5adf95ebbeab",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/d5b3851d0d74c8482c267762e70caf7f58f4f293"
        },
        "date": 1781770124576,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2877.724522908529,
            "unit": "ns",
            "range": "± 41.714966266908114"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2645.3102060953775,
            "unit": "ns",
            "range": "± 73.06893053940667"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 81.91422325372696,
            "unit": "ns",
            "range": "± 1.2708823371452294"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.33123358090719,
            "unit": "ns",
            "range": "± 0.10586528219272029"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 394.82743151982623,
            "unit": "ns",
            "range": "± 1.257910956226256"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 663.3418706258138,
            "unit": "ns",
            "range": "± 7.408943974759196"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.553088504821062,
            "unit": "ns",
            "range": "± 0.011153648683971139"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 257.9666744867961,
            "unit": "ns",
            "range": "± 2.765729418597633"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "972a7e1543b09e0ba9a5455b1560145ed80292c1",
          "message": "Implemented the benchmark/load-test expansion end to end",
          "timestamp": "2026-06-18T11:05:58+02:00",
          "tree_id": "485ea28888c96763ab19ee064ec892b43578e8cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/972a7e1543b09e0ba9a5455b1560145ed80292c1"
        },
        "date": 1781773898950,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.4495779449741046,
            "unit": "ns",
            "range": "± 0.2028458847584128"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.63021604220073,
            "unit": "ns",
            "range": "± 0.31747546826494366"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 143.91884152094522,
            "unit": "ns",
            "range": "± 0.024783612496389922"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 3207.266279856364,
            "unit": "ns",
            "range": "± 21.34179888098583"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2976.2003758748374,
            "unit": "ns",
            "range": "± 27.940390958881878"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 82.04129966100056,
            "unit": "ns",
            "range": "± 0.12835829699959836"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 59.881337424119316,
            "unit": "ns",
            "range": "± 0.9606389934116849"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2961.7411499023438,
            "unit": "ns",
            "range": "± 2.756073375027038"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 4025.1401087443032,
            "unit": "ns",
            "range": "± 5.10504390204372"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 3221.1912218729653,
            "unit": "ns",
            "range": "± 69.52715569419686"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 10321.103185017904,
            "unit": "ns",
            "range": "± 61.39419683701608"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 3099.1863848368325,
            "unit": "ns",
            "range": "± 79.27336977308326"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 3994.7254333496094,
            "unit": "ns",
            "range": "± 15.077938949981357"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 8357.000966389975,
            "unit": "ns",
            "range": "± 23.272847930132293"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23504.135899861652,
            "unit": "ns",
            "range": "± 165.7703891832507"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 3024.686522165934,
            "unit": "ns",
            "range": "± 5.310523978401698"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 3974.615867614746,
            "unit": "ns",
            "range": "± 8.258135745606403"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 27517.459050496418,
            "unit": "ns",
            "range": "± 35.67756190442506"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74525.0039876302,
            "unit": "ns",
            "range": "± 679.0390048601942"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 352.6823550860087,
            "unit": "ns",
            "range": "± 3.191398592668586"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 611.9915281931559,
            "unit": "ns",
            "range": "± 18.134496253174373"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6950507300595443,
            "unit": "ns",
            "range": "± 0.036651693192257945"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 235.0240425268809,
            "unit": "ns",
            "range": "± 1.311990915209718"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 247.04992818832397,
            "unit": "ns",
            "range": "± 9.4957729952684"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29309.894083658855,
            "unit": "ns",
            "range": "± 67.70316291638666"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 1363.7298676172893,
            "unit": "ns",
            "range": "± 0.08917223716355285"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 123.5879356066386,
            "unit": "ns",
            "range": "± 0.3939011607556087"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 489.6007095972697,
            "unit": "ns",
            "range": "± 3.8005297792494352"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 238.4777309099833,
            "unit": "ns",
            "range": "± 0.38360998675242425"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 245938.8400065104,
            "unit": "ns",
            "range": "± 461.0961113723102"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 11558.48269144694,
            "unit": "ns",
            "range": "± 138.0790021022362"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 156.2004535595576,
            "unit": "ns",
            "range": "± 0.8669955211943384"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 490.82854557037354,
            "unit": "ns",
            "range": "± 1.2035411435533843"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 241.69647852579752,
            "unit": "ns",
            "range": "± 0.5719406921101645"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2359987.8463541665,
            "unit": "ns",
            "range": "± 18391.891738690087"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 79806.43900553386,
            "unit": "ns",
            "range": "± 116.88903697884778"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 121.2769695520401,
            "unit": "ns",
            "range": "± 0.6739921068172239"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 487.3466698328654,
            "unit": "ns",
            "range": "± 11.486969733889076"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "972a7e1543b09e0ba9a5455b1560145ed80292c1",
          "message": "Implemented the benchmark/load-test expansion end to end",
          "timestamp": "2026-06-18T11:05:58+02:00",
          "tree_id": "485ea28888c96763ab19ee064ec892b43578e8cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/972a7e1543b09e0ba9a5455b1560145ed80292c1"
        },
        "date": 1781775468722,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0,
            "unit": "ns",
            "range": "± 0"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 72.02649025122325,
            "unit": "ns",
            "range": "± 0.43663034458454925"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 146.70412484804788,
            "unit": "ns",
            "range": "± 1.6503939403672716"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 3239.4806543986,
            "unit": "ns",
            "range": "± 1.4980540260751212"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2993.455369313558,
            "unit": "ns",
            "range": "± 85.45887612745187"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 85.17668120066325,
            "unit": "ns",
            "range": "± 1.4865139202603683"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 60.01202573378881,
            "unit": "ns",
            "range": "± 0.7972047134034079"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 3000.5942827860513,
            "unit": "ns",
            "range": "± 6.73082466432732"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 4026.6197814941406,
            "unit": "ns",
            "range": "± 17.072083835871382"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 3169.7946141560874,
            "unit": "ns",
            "range": "± 4.090504244292411"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 10050.91269938151,
            "unit": "ns",
            "range": "± 205.65775751195233"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 3027.555475870768,
            "unit": "ns",
            "range": "± 81.93146602049126"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 3940.0512568155923,
            "unit": "ns",
            "range": "± 12.121419794504606"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 8319.531244913736,
            "unit": "ns",
            "range": "± 16.710396555563065"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23469.737864176434,
            "unit": "ns",
            "range": "± 67.75528192523781"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 3066.1200803120933,
            "unit": "ns",
            "range": "± 3.3768150167516784"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 4043.1282094319663,
            "unit": "ns",
            "range": "± 10.961041124841223"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 28311.842981974285,
            "unit": "ns",
            "range": "± 57.52232921934212"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74649.11104329427,
            "unit": "ns",
            "range": "± 1182.4487378025583"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 352.28114652633667,
            "unit": "ns",
            "range": "± 2.0605672702125712"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 614.9682315190634,
            "unit": "ns",
            "range": "± 2.543436081983064"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6732592433691025,
            "unit": "ns",
            "range": "± 0.0019491347400924053"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 240.85796705881754,
            "unit": "ns",
            "range": "± 0.6532081523755947"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 241.9299988746643,
            "unit": "ns",
            "range": "± 0.41160380879272246"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29571.171244303387,
            "unit": "ns",
            "range": "± 211.111680788486"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 1379.6573187510173,
            "unit": "ns",
            "range": "± 9.061376021639354"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 118.60943416754405,
            "unit": "ns",
            "range": "± 0.20614265116613006"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 513.1132100423177,
            "unit": "ns",
            "range": "± 7.313752459685129"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 246.3474801381429,
            "unit": "ns",
            "range": "± 7.253507088766464"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 254135.1551106771,
            "unit": "ns",
            "range": "± 926.4701554072036"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 11087.482241312662,
            "unit": "ns",
            "range": "± 46.62785467094978"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 126.38647198677063,
            "unit": "ns",
            "range": "± 0.6879011811592364"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 496.18494669596356,
            "unit": "ns",
            "range": "± 9.30458500280152"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 243.91294940312704,
            "unit": "ns",
            "range": "± 0.4862531487548039"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2434365.4010416665,
            "unit": "ns",
            "range": "± 5661.577643330376"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 76949.45992024739,
            "unit": "ns",
            "range": "± 285.30786540930984"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 124.71054418881734,
            "unit": "ns",
            "range": "± 0.2553872835199892"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 493.7848898569743,
            "unit": "ns",
            "range": "± 4.7943193574623315"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e",
          "message": "fixed in-memory waiter timer could race cleanup",
          "timestamp": "2026-06-18T11:55:14+02:00",
          "tree_id": "b367b0e04256a6f5a8564598ae67835b375bc3d6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e"
        },
        "date": 1781776839762,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.3711882531642914,
            "unit": "ns",
            "range": "± 0.04411558994231921"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 80.37158942222595,
            "unit": "ns",
            "range": "± 0.18769167165780645"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 154.2345825036367,
            "unit": "ns",
            "range": "± 0.2243681134729174"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2321.3618710835776,
            "unit": "ns",
            "range": "± 2.745898703958519"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2136.958812713623,
            "unit": "ns",
            "range": "± 4.659353116930605"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 85.87010349829991,
            "unit": "ns",
            "range": "± 0.7143679144819163"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 56.33647578954697,
            "unit": "ns",
            "range": "± 0.0885880111137694"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2159.600596110026,
            "unit": "ns",
            "range": "± 8.412732162337056"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2972.513673146566,
            "unit": "ns",
            "range": "± 3.1431794350240385"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2320.0534477233887,
            "unit": "ns",
            "range": "± 0.6424859815811438"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9329.71021525065,
            "unit": "ns",
            "range": "± 46.622931954117725"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2152.6380666097007,
            "unit": "ns",
            "range": "± 19.526200430868908"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2949.8375536600747,
            "unit": "ns",
            "range": "± 2.53330800384297"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6215.492937723796,
            "unit": "ns",
            "range": "± 8.653838101349649"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23514.650583902996,
            "unit": "ns",
            "range": "± 251.98960621476886"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2147.0819714864097,
            "unit": "ns",
            "range": "± 8.248474657146383"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 3015.230280558268,
            "unit": "ns",
            "range": "± 5.218882751442388"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 20729.3559773763,
            "unit": "ns",
            "range": "± 58.50809943104488"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 73871.49820963542,
            "unit": "ns",
            "range": "± 837.4722926216423"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 399.00262053807575,
            "unit": "ns",
            "range": "± 0.5919871614302695"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 687.1972808837891,
            "unit": "ns",
            "range": "± 5.860356266191662"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.1614982510606449,
            "unit": "ns",
            "range": "± 0.0011234007882762094"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 252.50015226999918,
            "unit": "ns",
            "range": "± 1.1218571434240912"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 256.28295516967773,
            "unit": "ns",
            "range": "± 0.5289088308246105"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29267.876210530598,
            "unit": "ns",
            "range": "± 27.79374257919013"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 1420.2448749542236,
            "unit": "ns",
            "range": "± 9.925041655999404"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 132.75927873452505,
            "unit": "ns",
            "range": "± 0.7247858843087722"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 544.793464342753,
            "unit": "ns",
            "range": "± 2.3055096744734342"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 254.00367736816406,
            "unit": "ns",
            "range": "± 3.6930806311954334"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 253846.359375,
            "unit": "ns",
            "range": "± 802.5159087774489"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 9659.682108561197,
            "unit": "ns",
            "range": "± 21.390308698284898"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 133.80262303352356,
            "unit": "ns",
            "range": "± 0.9898399945055052"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 557.6639540990194,
            "unit": "ns",
            "range": "± 10.699471388981339"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 250.27028195063272,
            "unit": "ns",
            "range": "± 0.3306547590226865"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2642587.4830729165,
            "unit": "ns",
            "range": "± 40504.1008540953"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 73518.26249186198,
            "unit": "ns",
            "range": "± 913.3676937200371"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 132.84861067930856,
            "unit": "ns",
            "range": "± 0.8286855073126906"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 538.6505044301351,
            "unit": "ns",
            "range": "± 2.383959405117365"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b",
          "message": "Implemented the performance/allocation",
          "timestamp": "2026-06-18T14:35:14+02:00",
          "tree_id": "a1588a9b54a77bae3df547ec3acfa5e1dc0ea3a7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b"
        },
        "date": 1781786620915,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.40850799282391864,
            "unit": "ns",
            "range": "± 0.032427271318866566"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 84.08645784854889,
            "unit": "ns",
            "range": "± 0.6541695889458987"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 165.01146189371744,
            "unit": "ns",
            "range": "± 0.9803077934306844"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 1954.8729871114094,
            "unit": "ns",
            "range": "± 3.930304088673207"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 1773.5996774037678,
            "unit": "ns",
            "range": "± 12.574367665556776"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 90.28378935654958,
            "unit": "ns",
            "range": "± 1.3551197441472929"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 56.3452071249485,
            "unit": "ns",
            "range": "± 3.299263509938923"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 1760.5716775258381,
            "unit": "ns",
            "range": "± 3.8488656370910954"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2589.8830032348633,
            "unit": "ns",
            "range": "± 6.770112941169234"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2007.6111221313477,
            "unit": "ns",
            "range": "± 14.136792242601553"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 11910.375,
            "unit": "ns",
            "range": "± 615.5964952531772"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 1755.8832314809163,
            "unit": "ns",
            "range": "± 3.732131774841434"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2611.9298680623374,
            "unit": "ns",
            "range": "± 10.664144210707436"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 5611.841862996419,
            "unit": "ns",
            "range": "± 18.702559311105823"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 28524.46908569336,
            "unit": "ns",
            "range": "± 209.810480636947"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 1779.6677366892498,
            "unit": "ns",
            "range": "± 23.519342338668846"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2605.5872840881348,
            "unit": "ns",
            "range": "± 5.493829847679832"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 19633.511647542316,
            "unit": "ns",
            "range": "± 217.41579691003497"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 83095.27945963542,
            "unit": "ns",
            "range": "± 617.4815723806463"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 410.1574300130208,
            "unit": "ns",
            "range": "± 3.0618447548423413"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 580.5816265741984,
            "unit": "ns",
            "range": "± 8.571964393542379"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.175598646203677,
            "unit": "ns",
            "range": "± 0.00033879468601796337"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 250.5469797452291,
            "unit": "ns",
            "range": "± 0.433492327828976"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 251.43021869659424,
            "unit": "ns",
            "range": "± 0.43236070277190364"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 28598.968200683594,
            "unit": "ns",
            "range": "± 68.02455864106082"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 948.4763228098551,
            "unit": "ns",
            "range": "± 8.251384486516075"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 145.0419131120046,
            "unit": "ns",
            "range": "± 0.4661236536125527"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 552.3671652475992,
            "unit": "ns",
            "range": "± 1.7802319423570772"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 253.83294041951498,
            "unit": "ns",
            "range": "± 0.8275216254322894"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 246752.478515625,
            "unit": "ns",
            "range": "± 966.1690548168464"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 8181.988672892253,
            "unit": "ns",
            "range": "± 11.922551983705432"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 142.54309074083963,
            "unit": "ns",
            "range": "± 1.0793987259620794"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 535.978551864624,
            "unit": "ns",
            "range": "± 5.52139625558673"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 252.5005874633789,
            "unit": "ns",
            "range": "± 0.4646978293914771"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2362652.755859375,
            "unit": "ns",
            "range": "± 28450.881138296692"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 65528.95056152344,
            "unit": "ns",
            "range": "± 48.58492213876763"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 141.22813125451407,
            "unit": "ns",
            "range": "± 0.9109238698896829"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 541.5926253000895,
            "unit": "ns",
            "range": "± 18.06271369484551"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "15bd5d6f3d11509bb5892de59d762889abbd5404",
          "message": "fix stress test harness",
          "timestamp": "2026-06-18T15:00:25+02:00",
          "tree_id": "991639655faaef8013e8004d3ff5bacaaa0ed662",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/15bd5d6f3d11509bb5892de59d762889abbd5404"
        },
        "date": 1781788111400,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.6681626215577126,
            "unit": "ns",
            "range": "± 0.011152961730592612"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.97365478674571,
            "unit": "ns",
            "range": "± 0.5358498789307601"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 158.04198575019836,
            "unit": "ns",
            "range": "± 1.4578672679683529"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2537.543847401937,
            "unit": "ns",
            "range": "± 9.79790162652527"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2300.559226989746,
            "unit": "ns",
            "range": "± 1.4550933466665585"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 76.46994866927464,
            "unit": "ns",
            "range": "± 0.9216520795043048"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.218242824077606,
            "unit": "ns",
            "range": "± 0.10012997724176449"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2284.9227002461753,
            "unit": "ns",
            "range": "± 20.97086369607238"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 3240.0269508361816,
            "unit": "ns",
            "range": "± 7.808589294625143"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2515.446679433187,
            "unit": "ns",
            "range": "± 33.230371034076555"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9136.587514241537,
            "unit": "ns",
            "range": "± 24.78709855935575"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2263.64644241333,
            "unit": "ns",
            "range": "± 1.2431342624951605"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 3287.209929148356,
            "unit": "ns",
            "range": "± 7.73109674139487"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6871.212760925293,
            "unit": "ns",
            "range": "± 5.07963798647935"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23423.00537109375,
            "unit": "ns",
            "range": "± 93.49159928875386"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2285.205149332682,
            "unit": "ns",
            "range": "± 1.1842462657700152"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 3238.6383425394692,
            "unit": "ns",
            "range": "± 24.972680907687906"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 24267.113037109375,
            "unit": "ns",
            "range": "± 111.33277229776296"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74008.49881998698,
            "unit": "ns",
            "range": "± 982.1129968582954"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 399.27735535303754,
            "unit": "ns",
            "range": "± 0.21802489835676145"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 596.1089677810669,
            "unit": "ns",
            "range": "± 4.175317855334668"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.8842867091298103,
            "unit": "ns",
            "range": "± 0.012385656840886198"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 255.36220836639404,
            "unit": "ns",
            "range": "± 0.61233022447099"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 265.0263492266337,
            "unit": "ns",
            "range": "± 0.7916979476495232"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30115.127365112305,
            "unit": "ns",
            "range": "± 482.5133497347164"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 946.2327667872111,
            "unit": "ns",
            "range": "± 2.9044436778809737"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 133.38911100228628,
            "unit": "ns",
            "range": "± 0.8422660204638421"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 513.5944245656332,
            "unit": "ns",
            "range": "± 1.1288171387033055"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 258.22424507141113,
            "unit": "ns",
            "range": "± 0.17307657751300898"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 279429.796875,
            "unit": "ns",
            "range": "± 1495.3858625392452"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 7345.277437845866,
            "unit": "ns",
            "range": "± 17.94565424079641"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 139.58091576894125,
            "unit": "ns",
            "range": "± 1.339464582335789"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 515.3137019475301,
            "unit": "ns",
            "range": "± 5.886028478155707"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 261.52248700459796,
            "unit": "ns",
            "range": "± 0.27720040219992975"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2559873.78125,
            "unit": "ns",
            "range": "± 12259.087062875984"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 59625.07354736328,
            "unit": "ns",
            "range": "± 19.281825600418422"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 136.79649710655212,
            "unit": "ns",
            "range": "± 1.2275695553398587"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 526.1927471160889,
            "unit": "ns",
            "range": "± 3.3475313263759614"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "677e678174e24eb024297dbe53c45145c2ecf137",
          "message": "Performance improvements",
          "timestamp": "2026-06-18T15:51:13+02:00",
          "tree_id": "af70395d47c9bcf88dc29a7d5f10ed4abe38030b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/677e678174e24eb024297dbe53c45145c2ecf137"
        },
        "date": 1781790997338,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.9608657484253248,
            "unit": "ns",
            "range": "± 0.20801842640995882"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 78.6515406370163,
            "unit": "ns",
            "range": "± 0.5240824054046532"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 157.00447289148966,
            "unit": "ns",
            "range": "± 0.42684058124561247"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2872.680955251058,
            "unit": "ns",
            "range": "± 20.29615439069068"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2566.439768473307,
            "unit": "ns",
            "range": "± 2.575002053298984"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 85.82323545217514,
            "unit": "ns",
            "range": "± 0.6140002171220142"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 58.127751568953194,
            "unit": "ns",
            "range": "± 0.8087353664460527"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2553.4136098225913,
            "unit": "ns",
            "range": "± 7.250207184958942"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2984.8583285013833,
            "unit": "ns",
            "range": "± 3.5238383797420685"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2784.049393971761,
            "unit": "ns",
            "range": "± 4.479211066334014"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9374.85161336263,
            "unit": "ns",
            "range": "± 60.80571398662222"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2572.042980194092,
            "unit": "ns",
            "range": "± 11.31685407900113"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 3017.044428507487,
            "unit": "ns",
            "range": "± 5.390524823407156"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 7726.5889892578125,
            "unit": "ns",
            "range": "± 56.25492593296578"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23247.382975260418,
            "unit": "ns",
            "range": "± 290.8078413335748"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2571.1365114847817,
            "unit": "ns",
            "range": "± 8.730859611810834"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2991.457146962484,
            "unit": "ns",
            "range": "± 9.066394507002897"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 26876.03628031413,
            "unit": "ns",
            "range": "± 120.62417458215826"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 79012.66719563802,
            "unit": "ns",
            "range": "± 1268.856473381973"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 314.2609163920085,
            "unit": "ns",
            "range": "± 0.8377536414160164"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 526.0896794001261,
            "unit": "ns",
            "range": "± 1.318656829010132"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.7092029104630153,
            "unit": "ns",
            "range": "± 0.022969687954623567"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 238.89011653264365,
            "unit": "ns",
            "range": "± 0.8879137624078661"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 244.8275990486145,
            "unit": "ns",
            "range": "± 0.41884379033884944"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29509.379302978516,
            "unit": "ns",
            "range": "± 186.40305880600008"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 890.7641235987345,
            "unit": "ns",
            "range": "± 4.289706865574242"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 137.31897115707397,
            "unit": "ns",
            "range": "± 1.0153468719765295"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 511.19226837158203,
            "unit": "ns",
            "range": "± 7.28766678312035"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 245.77890284856161,
            "unit": "ns",
            "range": "± 0.6247863883776154"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 257177.5380859375,
            "unit": "ns",
            "range": "± 1059.5739905026214"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 7632.553049723308,
            "unit": "ns",
            "range": "± 17.384990534834074"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 134.0607245763143,
            "unit": "ns",
            "range": "± 1.5732525667017931"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 477.56457297007245,
            "unit": "ns",
            "range": "± 0.9008161553879915"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 249.59658432006836,
            "unit": "ns",
            "range": "± 1.1709722228944395"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2420878.7317708335,
            "unit": "ns",
            "range": "± 24930.34914695145"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 64218.8330485026,
            "unit": "ns",
            "range": "± 1616.9122237556626"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 126.11837017536163,
            "unit": "ns",
            "range": "± 0.20157631436993378"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 545.8880790074667,
            "unit": "ns",
            "range": "± 7.658650686639676"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "394284cca7942c20224fd400d3e489fbe22529d4",
          "message": "Restored the typed DispatchResponseCoreAsync",
          "timestamp": "2026-06-18T19:10:42+02:00",
          "tree_id": "6a2302c9e30b9081bd4153efd804c14d2ad8f0d0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/394284cca7942c20224fd400d3e489fbe22529d4"
        },
        "date": 1781803133646,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.6700939014554024,
            "unit": "ns",
            "range": "± 0.013004563650454054"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 77.83681255578995,
            "unit": "ns",
            "range": "± 0.7291831629071919"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 162.22746435801187,
            "unit": "ns",
            "range": "± 0.34366014767203856"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2508.472957611084,
            "unit": "ns",
            "range": "± 5.175205529882771"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2288.900380452474,
            "unit": "ns",
            "range": "± 5.784310176287072"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 78.28857614596684,
            "unit": "ns",
            "range": "± 1.389021064988462"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.37301335732142,
            "unit": "ns",
            "range": "± 0.10943213548985355"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2287.4485766092935,
            "unit": "ns",
            "range": "± 3.4096386649588877"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2694.5829582214355,
            "unit": "ns",
            "range": "± 2.826229159686116"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2496.155132293701,
            "unit": "ns",
            "range": "± 1.4067523576408882"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9096.663599650064,
            "unit": "ns",
            "range": "± 25.81457351291688"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2285.8215942382812,
            "unit": "ns",
            "range": "± 2.834301923098183"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2687.421502431234,
            "unit": "ns",
            "range": "± 3.6991543054339435"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6908.840263366699,
            "unit": "ns",
            "range": "± 24.78004146739714"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23814.3828125,
            "unit": "ns",
            "range": "± 270.04816299470787"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2288.2096519470215,
            "unit": "ns",
            "range": "± 5.628200696449136"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2672.5762672424316,
            "unit": "ns",
            "range": "± 12.466798772750744"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23969.304641723633,
            "unit": "ns",
            "range": "± 101.43178937759515"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 71207.369140625,
            "unit": "ns",
            "range": "± 692.7412591809613"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 345.62986437479657,
            "unit": "ns",
            "range": "± 0.6509037045764954"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 562.2903703053793,
            "unit": "ns",
            "range": "± 0.8838856380830897"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5436103492975235,
            "unit": "ns",
            "range": "± 0.0013563044613611429"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 250.33413807551065,
            "unit": "ns",
            "range": "± 0.6466906863308132"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 274.07993745803833,
            "unit": "ns",
            "range": "± 1.1903355745409017"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29694.27079264323,
            "unit": "ns",
            "range": "± 50.141441757859965"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 930.7990182240804,
            "unit": "ns",
            "range": "± 1.360378064750734"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 132.4390854438146,
            "unit": "ns",
            "range": "± 0.6534612959837937"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 513.7141695022583,
            "unit": "ns",
            "range": "± 6.964130718574672"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 269.3697066307068,
            "unit": "ns",
            "range": "± 0.8730200249444343"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 256390.0196940104,
            "unit": "ns",
            "range": "± 1135.9596737016163"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 7315.252072652181,
            "unit": "ns",
            "range": "± 2.197076779625879"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 133.56375241279602,
            "unit": "ns",
            "range": "± 0.16880049051465157"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 500.30014578501385,
            "unit": "ns",
            "range": "± 2.551233703552353"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 265.135587533315,
            "unit": "ns",
            "range": "± 0.3978366262065024"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2514401.796875,
            "unit": "ns",
            "range": "± 18245.84106077326"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 59302.35266113281,
            "unit": "ns",
            "range": "± 28.32523889957488"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 133.0038456916809,
            "unit": "ns",
            "range": "± 1.2022001696217284"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 504.383905728658,
            "unit": "ns",
            "range": "± 6.072747794071544"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1bc203254299568b68f861be578ab6ac00ba7f06",
          "message": "extensive test coverage",
          "timestamp": "2026-06-18T20:04:38+02:00",
          "tree_id": "689e907f6b8413b6cd79ed03ad31498cb02d2b7a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1bc203254299568b68f861be578ab6ac00ba7f06"
        },
        "date": 1781806365721,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.2721751065303882,
            "unit": "ns",
            "range": "± 0.0005880110856433244"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 59.05954656998316,
            "unit": "ns",
            "range": "± 0.20891927366513585"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 117.96511614322662,
            "unit": "ns",
            "range": "± 0.3166840956969766"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2164.7778244018555,
            "unit": "ns",
            "range": "± 3.2481557553685234"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 1971.5441652933757,
            "unit": "ns",
            "range": "± 2.092824671975301"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 64.81353569030762,
            "unit": "ns",
            "range": "± 0.5818495462121062"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 46.11187808712324,
            "unit": "ns",
            "range": "± 0.13835215495692227"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 1989.413496653239,
            "unit": "ns",
            "range": "± 34.41737941135403"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2315.8503023783364,
            "unit": "ns",
            "range": "± 20.74854396774604"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2174.0196100870767,
            "unit": "ns",
            "range": "± 7.470461774965551"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 7631.1124928792315,
            "unit": "ns",
            "range": "± 66.40389169308963"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 1955.8822199503581,
            "unit": "ns",
            "range": "± 2.2853167802809726"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2334.4555117289224,
            "unit": "ns",
            "range": "± 16.49167875281956"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 5902.023895263672,
            "unit": "ns",
            "range": "± 2.4173692201944172"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 17662.377507527668,
            "unit": "ns",
            "range": "± 265.92344838291376"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 1958.995226542155,
            "unit": "ns",
            "range": "± 6.3274411127432435"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2294.3625132242837,
            "unit": "ns",
            "range": "± 2.9027258501611324"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 20267.96143086751,
            "unit": "ns",
            "range": "± 87.2008363047264"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 57238.61354573568,
            "unit": "ns",
            "range": "± 411.93165791419784"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 229.71376291910806,
            "unit": "ns",
            "range": "± 0.5339030111861035"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 402.46302604675293,
            "unit": "ns",
            "range": "± 1.1516022432311381"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.2980475028355916,
            "unit": "ns",
            "range": "± 0.001417383440264274"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 179.48912318547568,
            "unit": "ns",
            "range": "± 0.2349189118080839"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 186.04645053545633,
            "unit": "ns",
            "range": "± 0.9452408664515516"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 21956.324513753254,
            "unit": "ns",
            "range": "± 87.77228971452767"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 673.6524095535278,
            "unit": "ns",
            "range": "± 0.5445605940531223"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 104.00084879000981,
            "unit": "ns",
            "range": "± 1.0077334287189434"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 362.82240835825604,
            "unit": "ns",
            "range": "± 1.2182523163611778"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 183.75440629323325,
            "unit": "ns",
            "range": "± 0.7884332665312928"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 194292.5497639974,
            "unit": "ns",
            "range": "± 1000.6900380355463"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 5776.945887247722,
            "unit": "ns",
            "range": "± 74.68318906240886"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 94.74629884958267,
            "unit": "ns",
            "range": "± 0.5187441682778762"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 374.1502267519633,
            "unit": "ns",
            "range": "± 7.3839443097376"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 184.1150373617808,
            "unit": "ns",
            "range": "± 0.6623895320094104"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 1821720.6022135417,
            "unit": "ns",
            "range": "± 8042.416420229817"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 47495.8583984375,
            "unit": "ns",
            "range": "± 39.670914544575375"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 96.1679828564326,
            "unit": "ns",
            "range": "± 0.033538151979659384"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 378.2301643689473,
            "unit": "ns",
            "range": "± 8.841072501875372"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b2b7198ad04d615dc67c887a8833153b5f000da4",
          "message": "Optimized context propagator and InMemoryRecoveryStateStore",
          "timestamp": "2026-06-18T20:59:24+02:00",
          "tree_id": "5dfc241eabebd6663bd8ddf74ea99542b923a9cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b2b7198ad04d615dc67c887a8833153b5f000da4"
        },
        "date": 1781809648957,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.1022239016989867,
            "unit": "ns",
            "range": "± 0.1770569914905714"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 59.688297390937805,
            "unit": "ns",
            "range": "± 0.09447045529012574"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 79.0607279141744,
            "unit": "ns",
            "range": "± 0.1256390442560022"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2214.9147415161133,
            "unit": "ns",
            "range": "± 4.590136655588579"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2018.7937647501628,
            "unit": "ns",
            "range": "± 51.80288413149937"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 67.38879849513371,
            "unit": "ns",
            "range": "± 3.2845685141269643"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 45.51337358355522,
            "unit": "ns",
            "range": "± 0.015352364827970897"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 1988.9033304850261,
            "unit": "ns",
            "range": "± 6.2842331461625145"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2376.779192606608,
            "unit": "ns",
            "range": "± 8.033162550645395"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2161.1558227539062,
            "unit": "ns",
            "range": "± 13.499371799292089"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 7668.3410237630205,
            "unit": "ns",
            "range": "± 122.83472030770638"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 1978.0044746398926,
            "unit": "ns",
            "range": "± 15.437520582019111"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2357.2332916259766,
            "unit": "ns",
            "range": "± 2.306695240036446"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 5973.6409962972,
            "unit": "ns",
            "range": "± 6.357088526626916"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 18152.518463134766,
            "unit": "ns",
            "range": "± 69.1934566642551"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 1996.4722137451172,
            "unit": "ns",
            "range": "± 4.528326024111162"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2358.2055155436196,
            "unit": "ns",
            "range": "± 3.792574792693507"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 20799.91862487793,
            "unit": "ns",
            "range": "± 106.43376943966766"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 58974.033630371094,
            "unit": "ns",
            "range": "± 318.52305511985344"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 232.50743436813354,
            "unit": "ns",
            "range": "± 2.635596073931356"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 404.4756700197856,
            "unit": "ns",
            "range": "± 0.45776368825990266"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.316257583598296,
            "unit": "ns",
            "range": "± 0.028781827330020184"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 183.52821000417075,
            "unit": "ns",
            "range": "± 0.5107048975808233"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 188.79622475306192,
            "unit": "ns",
            "range": "± 0.5686272222172776"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 23817.796864827473,
            "unit": "ns",
            "range": "± 72.20900232862635"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 487.11771233876544,
            "unit": "ns",
            "range": "± 8.391621625823"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 95.49278064568837,
            "unit": "ns",
            "range": "± 0.5003276306681076"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 399.0435085296631,
            "unit": "ns",
            "range": "± 3.8755842273873853"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 199.33889881769815,
            "unit": "ns",
            "range": "± 1.1670595806488395"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 204588.65559895834,
            "unit": "ns",
            "range": "± 766.4098632949881"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3126.647444407145,
            "unit": "ns",
            "range": "± 168.1409677324379"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 94.43692940473557,
            "unit": "ns",
            "range": "± 0.5011657232505202"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 385.18807458877563,
            "unit": "ns",
            "range": "± 10.67186990998489"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 187.6927239894867,
            "unit": "ns",
            "range": "± 0.7799940191208287"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 1904845.9921875,
            "unit": "ns",
            "range": "± 5776.390927341525"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 21240.768432617188,
            "unit": "ns",
            "range": "± 297.03178593095197"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 97.47748557726543,
            "unit": "ns",
            "range": "± 0.5806462569803982"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 384.11977910995483,
            "unit": "ns",
            "range": "± 6.237407637617714"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ba59b12ce2698fbd413df7e07a67c7581f23b3fd",
          "message": "Fixed SetResponseCore/SetRawResponseJsonCore code duplication",
          "timestamp": "2026-06-19T11:33:23+02:00",
          "tree_id": "a38526b7b76cd516a7f2fd80dd5f06f0b5380b1a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ba59b12ce2698fbd413df7e07a67c7581f23b3fd"
        },
        "date": 1781862065905,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.22011243924498558,
            "unit": "ns",
            "range": "± 0.002593374969959109"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 75.59138840436935,
            "unit": "ns",
            "range": "± 0.5299922874638731"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 96.93188506364822,
            "unit": "ns",
            "range": "± 0.3352128437192566"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2496.383586883545,
            "unit": "ns",
            "range": "± 1.899725292517885"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2248.298646291097,
            "unit": "ns",
            "range": "± 4.670512565542979"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 76.13230419158936,
            "unit": "ns",
            "range": "± 1.5402159609302633"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.17979174852371,
            "unit": "ns",
            "range": "± 0.09514930654971412"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2293.149311065674,
            "unit": "ns",
            "range": "± 29.466327402307595"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2667.3271611531577,
            "unit": "ns",
            "range": "± 0.7451403660845125"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2501.1672274271646,
            "unit": "ns",
            "range": "± 2.8867323130502567"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9265.909474690756,
            "unit": "ns",
            "range": "± 123.56467341185173"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2285.5112113952637,
            "unit": "ns",
            "range": "± 4.401554261919708"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2689.103209177653,
            "unit": "ns",
            "range": "± 3.6415038676029945"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6867.544235229492,
            "unit": "ns",
            "range": "± 30.049363247606557"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23531.4111328125,
            "unit": "ns",
            "range": "± 418.4595678197604"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2323.686250050863,
            "unit": "ns",
            "range": "± 10.818980771410276"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2680.8627281188965,
            "unit": "ns",
            "range": "± 2.6875189214219195"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23770.50812784831,
            "unit": "ns",
            "range": "± 27.61557829147051"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 71154.1190592448,
            "unit": "ns",
            "range": "± 574.5118075886878"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 365.9958866437276,
            "unit": "ns",
            "range": "± 1.7378368315394181"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 561.4766047795614,
            "unit": "ns",
            "range": "± 0.9907310510690631"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5452617965638638,
            "unit": "ns",
            "range": "± 0.00323808120636314"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 250.0020988782247,
            "unit": "ns",
            "range": "± 1.4905857907572926"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 265.8995207150777,
            "unit": "ns",
            "range": "± 0.8942698525271215"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30251.43148803711,
            "unit": "ns",
            "range": "± 63.71938285697084"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 645.5011418660482,
            "unit": "ns",
            "range": "± 6.1422187019856915"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 133.2705353895823,
            "unit": "ns",
            "range": "± 0.975504557012255"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 524.8462359110514,
            "unit": "ns",
            "range": "± 4.034016857727801"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 268.2784520785014,
            "unit": "ns",
            "range": "± 0.7130539941817886"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 273352.89599609375,
            "unit": "ns",
            "range": "± 1167.7719186536958"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3782.2166849772134,
            "unit": "ns",
            "range": "± 35.26807903783459"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 131.63558022181192,
            "unit": "ns",
            "range": "± 0.9172525171005488"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 504.24735673268634,
            "unit": "ns",
            "range": "± 5.2639811190721195"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 269.57785590489704,
            "unit": "ns",
            "range": "± 0.803518165248622"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2542898.77734375,
            "unit": "ns",
            "range": "± 20065.248972880294"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 28638.865376790363,
            "unit": "ns",
            "range": "± 131.05607343914562"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 133.92288426558176,
            "unit": "ns",
            "range": "± 0.46320457707568274"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 523.2459506988525,
            "unit": "ns",
            "range": "± 5.869957455720521"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a",
          "message": "Fixed latency regression",
          "timestamp": "2026-06-19T12:12:07+02:00",
          "tree_id": "26379ae08cbe1cd71814d960a21bc6579c351a33",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a"
        },
        "date": 1781864275907,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.27362223093708354,
            "unit": "ns",
            "range": "± 0.00033187886695121424"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 70.10234713554382,
            "unit": "ns",
            "range": "± 0.5315508948956189"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 96.23164131244023,
            "unit": "ns",
            "range": "± 0.24386584738815498"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2483.8035456339517,
            "unit": "ns",
            "range": "± 6.83258008318221"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2291.0997886657715,
            "unit": "ns",
            "range": "± 3.6394422487761324"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 72.96072574456532,
            "unit": "ns",
            "range": "± 0.21188042616549319"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 57.33467083175977,
            "unit": "ns",
            "range": "± 0.6765586806101482"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2291.5354766845703,
            "unit": "ns",
            "range": "± 3.7603756697818986"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2612.487471262614,
            "unit": "ns",
            "range": "± 3.4206845754588167"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2453.303326924642,
            "unit": "ns",
            "range": "± 4.1088051899241576"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9204.196594238281,
            "unit": "ns",
            "range": "± 254.37039051628358"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2263.0148646036782,
            "unit": "ns",
            "range": "± 2.025602960533456"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2639.3016306559243,
            "unit": "ns",
            "range": "± 3.4970285752226404"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6757.208320617676,
            "unit": "ns",
            "range": "± 8.033839847842732"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23560.099197387695,
            "unit": "ns",
            "range": "± 146.0548378122932"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2274.8226839701333,
            "unit": "ns",
            "range": "± 4.055743821932573"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2662.6239115397134,
            "unit": "ns",
            "range": "± 6.862505139900032"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23851.35301208496,
            "unit": "ns",
            "range": "± 125.04811573238615"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 72311.06013997395,
            "unit": "ns",
            "range": "± 1423.946728482796"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 345.7135451634725,
            "unit": "ns",
            "range": "± 0.7043766185810497"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 550.2284631729126,
            "unit": "ns",
            "range": "± 2.793469316243394"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6834098435938358,
            "unit": "ns",
            "range": "± 0.002022954073253646"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 244.4436968167623,
            "unit": "ns",
            "range": "± 1.31337514229032"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 252.07227381070456,
            "unit": "ns",
            "range": "± 0.464855710110835"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29076.8909962972,
            "unit": "ns",
            "range": "± 27.083644804472126"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 626.1754121780396,
            "unit": "ns",
            "range": "± 2.7411970086919433"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 114.86441091696422,
            "unit": "ns",
            "range": "± 0.147696153850588"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 463.3720636367798,
            "unit": "ns",
            "range": "± 1.1813026154276347"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 258.60647710164386,
            "unit": "ns",
            "range": "± 6.656689913518009"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 252739.59244791666,
            "unit": "ns",
            "range": "± 920.6697994648348"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3618.411289215088,
            "unit": "ns",
            "range": "± 54.5689653203692"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 116.8084848721822,
            "unit": "ns",
            "range": "± 0.1668697735427987"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 459.39809703826904,
            "unit": "ns",
            "range": "± 0.6129162953905105"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 254.80139048894247,
            "unit": "ns",
            "range": "± 1.635461519231939"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2370375.0846354165,
            "unit": "ns",
            "range": "± 7769.540610495018"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 25219.925231933594,
            "unit": "ns",
            "range": "± 76.71695972657722"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 117.20760464668274,
            "unit": "ns",
            "range": "± 0.40313667627072935"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 481.86870797475177,
            "unit": "ns",
            "range": "± 0.3374022772720713"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a95672f4df5a479d9f48b963078061da6aa84509",
          "message": "SetResponse hot path optimization",
          "timestamp": "2026-06-19T12:57:06+02:00",
          "tree_id": "1f0bcae2122dbcfdead7271a7134b90f5ffed9dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a95672f4df5a479d9f48b963078061da6aa84509"
        },
        "date": 1781867075211,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21965535109241804,
            "unit": "ns",
            "range": "± 0.005849832518341261"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.69406008720398,
            "unit": "ns",
            "range": "± 0.10244025761555438"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 92.99665343761444,
            "unit": "ns",
            "range": "± 0.2739432217270656"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2457.96240234375,
            "unit": "ns",
            "range": "± 33.94499755056095"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2227.838736216227,
            "unit": "ns",
            "range": "± 11.987344756676322"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 75.32393336296082,
            "unit": "ns",
            "range": "± 1.0877004049751615"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.66891096035639,
            "unit": "ns",
            "range": "± 0.17024973036616892"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2212.79829788208,
            "unit": "ns",
            "range": "± 6.15957968884152"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2573.761255900065,
            "unit": "ns",
            "range": "± 10.244655827669288"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2412.301223754883,
            "unit": "ns",
            "range": "± 8.17526792457308"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 8886.902951558432,
            "unit": "ns",
            "range": "± 36.36863038382258"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2201.201544443766,
            "unit": "ns",
            "range": "± 4.118736644019399"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2571.46662012736,
            "unit": "ns",
            "range": "± 7.661767875970989"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6627.688807169597,
            "unit": "ns",
            "range": "± 17.868272724346618"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23165.22715250651,
            "unit": "ns",
            "range": "± 273.967817168441"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2233.935159047445,
            "unit": "ns",
            "range": "± 3.8373661684222466"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2568.1068725585938,
            "unit": "ns",
            "range": "± 1.7301760502646784"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23249.9731241862,
            "unit": "ns",
            "range": "± 295.53465463290877"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 69969.07824707031,
            "unit": "ns",
            "range": "± 620.5652955948847"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 337.5075532595317,
            "unit": "ns",
            "range": "± 0.5852268880092344"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 542.8993269602457,
            "unit": "ns",
            "range": "± 1.568601363949287"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5466882772743702,
            "unit": "ns",
            "range": "± 0.0017202168627453202"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 255.14111296335855,
            "unit": "ns",
            "range": "± 0.9795481620711353"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 256.3692895571391,
            "unit": "ns",
            "range": "± 1.1068400648464582"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29381.013305664062,
            "unit": "ns",
            "range": "± 76.22763131489036"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 609.4314641952515,
            "unit": "ns",
            "range": "± 1.6940187810050742"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 124.72536782423656,
            "unit": "ns",
            "range": "± 0.14458930527667158"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 472.0602970123291,
            "unit": "ns",
            "range": "± 1.5477550924513042"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 259.7656059265137,
            "unit": "ns",
            "range": "± 0.5540090243731082"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 251006.333984375,
            "unit": "ns",
            "range": "± 1548.9606294562434"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3589.420716603597,
            "unit": "ns",
            "range": "± 11.963525117742877"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 126.33659187952678,
            "unit": "ns",
            "range": "± 1.4012409799896972"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 470.8266854286194,
            "unit": "ns",
            "range": "± 1.808713263509665"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 258.30715974171954,
            "unit": "ns",
            "range": "± 2.150142805934331"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2369589.1953125,
            "unit": "ns",
            "range": "± 7582.076977194389"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 25831.725184122723,
            "unit": "ns",
            "range": "± 64.290714333745"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 117.08807977040608,
            "unit": "ns",
            "range": "± 0.3859487325523787"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 477.3183526992798,
            "unit": "ns",
            "range": "± 8.382694037570948"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b5b9a73f55bc399e5ec9114ed56d830f24d4d855",
          "message": "InMemoryAsyncResponseChannel perf optimization",
          "timestamp": "2026-06-19T13:53:24+02:00",
          "tree_id": "430c87e41f483954b7a059e3043d430bf4efa341",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b5b9a73f55bc399e5ec9114ed56d830f24d4d855"
        },
        "date": 1781870468137,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.036735787987709045,
            "unit": "ns",
            "range": "± 0.00854461372228014"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 82.61482218901317,
            "unit": "ns",
            "range": "± 0.31790651578280815"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 107.36526195208232,
            "unit": "ns",
            "range": "± 0.2277727126152232"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 1931.033940633138,
            "unit": "ns",
            "range": "± 3.17808186036793"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 1754.9201997121174,
            "unit": "ns",
            "range": "± 0.9548529388226868"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 88.44184889396031,
            "unit": "ns",
            "range": "± 0.643314642010748"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 53.439159681399666,
            "unit": "ns",
            "range": "± 0.1298116661550835"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 1735.057056427002,
            "unit": "ns",
            "range": "± 0.8732032208186341"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2040.401730855306,
            "unit": "ns",
            "range": "± 6.556498709272991"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 1905.8081709543865,
            "unit": "ns",
            "range": "± 2.8721920567971475"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 8827.115051269531,
            "unit": "ns",
            "range": "± 175.84765623766847"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 1736.098425547282,
            "unit": "ns",
            "range": "± 3.03088544006938"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2082.85179011027,
            "unit": "ns",
            "range": "± 5.166432778410485"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 5502.202110290527,
            "unit": "ns",
            "range": "± 16.853866735431772"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23118.520212809246,
            "unit": "ns",
            "range": "± 603.9442640393835"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 1759.718510945638,
            "unit": "ns",
            "range": "± 11.704374311252298"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2078.326764424642,
            "unit": "ns",
            "range": "± 6.1995227327285996"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 19027.677841186523,
            "unit": "ns",
            "range": "± 101.55857500437303"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 69484.48236083984,
            "unit": "ns",
            "range": "± 730.8944032833881"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 344.3432065645854,
            "unit": "ns",
            "range": "± 1.8214574435488693"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 569.0022716522217,
            "unit": "ns",
            "range": "± 1.610887507743764"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.1657184300323327,
            "unit": "ns",
            "range": "± 0.0009617558524810917"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 246.0849528312683,
            "unit": "ns",
            "range": "± 0.7309172170291048"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 253.33763074874878,
            "unit": "ns",
            "range": "± 0.23867506508219435"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29497.081461588543,
            "unit": "ns",
            "range": "± 65.57708223905624"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 669.9331086476644,
            "unit": "ns",
            "range": "± 1.7557460325343892"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 131.59824792544046,
            "unit": "ns",
            "range": "± 1.4313422174927635"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 559.9140303929647,
            "unit": "ns",
            "range": "± 23.232032561496492"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 249.5882102648417,
            "unit": "ns",
            "range": "± 0.20629992795634933"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 253894.68798828125,
            "unit": "ns",
            "range": "± 715.24824218524"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 4534.8347727457685,
            "unit": "ns",
            "range": "± 117.62014878646353"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 131.92181313037872,
            "unit": "ns",
            "range": "± 0.13338956166735294"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 525.4645700454712,
            "unit": "ns",
            "range": "± 6.034972217470966"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 250.37865161895752,
            "unit": "ns",
            "range": "± 0.3104269599783623"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2615059.1087239585,
            "unit": "ns",
            "range": "± 14421.789438612286"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 36412.75362141927,
            "unit": "ns",
            "range": "± 842.1190738211037"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 145.6020770072937,
            "unit": "ns",
            "range": "± 0.9481480557160599"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 535.6551351547241,
            "unit": "ns",
            "range": "± 2.404060547550181"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "fe36405f03e470bc58ff4f78f9f2ba20725d526d",
          "message": "Implemented the opt-in Google Pub/Sub early-ACK path",
          "timestamp": "2026-06-19T15:46:06+02:00",
          "tree_id": "b43d143ce70f99303c7376e6623a0ff6882088e9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/fe36405f03e470bc58ff4f78f9f2ba20725d526d"
        },
        "date": 1781877146996,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21277880171934763,
            "unit": "ns",
            "range": "± 0.0027603940714577896"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 79.00303830703099,
            "unit": "ns",
            "range": "± 1.3494231315083307"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 99.58831570545833,
            "unit": "ns",
            "range": "± 0.29982896887677557"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2493.9550692240396,
            "unit": "ns",
            "range": "± 4.681633626896812"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2249.867027282715,
            "unit": "ns",
            "range": "± 5.670409883010713"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 80.41348663965861,
            "unit": "ns",
            "range": "± 1.9625973898292957"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 59.916438480218254,
            "unit": "ns",
            "range": "± 0.04738046054645678"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2266.5739466349282,
            "unit": "ns",
            "range": "± 4.935505351852691"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2612.1881980895996,
            "unit": "ns",
            "range": "± 13.778061353124691"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2460.151320139567,
            "unit": "ns",
            "range": "± 11.219340055319414"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9107.522493998209,
            "unit": "ns",
            "range": "± 70.86133547940932"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2259.845816294352,
            "unit": "ns",
            "range": "± 12.314645070786169"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2623.022206624349,
            "unit": "ns",
            "range": "± 5.836670088253326"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6851.683133443196,
            "unit": "ns",
            "range": "± 18.270171711831544"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23169.04835510254,
            "unit": "ns",
            "range": "± 218.11296121548338"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2273.6887486775718,
            "unit": "ns",
            "range": "± 2.4706442093301555"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2609.614289601644,
            "unit": "ns",
            "range": "± 3.1869211430623268"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23765.543126424152,
            "unit": "ns",
            "range": "± 81.8196320283971"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74239.31319173177,
            "unit": "ns",
            "range": "± 2388.3355509545972"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 359.89052867889404,
            "unit": "ns",
            "range": "± 0.4710905713813619"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 589.1915938059489,
            "unit": "ns",
            "range": "± 1.350021499172148"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5513152455290158,
            "unit": "ns",
            "range": "± 0.0032917909300576543"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 248.1247607866923,
            "unit": "ns",
            "range": "± 0.3636870413276204"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 37.98991390069326,
            "unit": "ns",
            "range": "± 0.22664464348401592"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 142.3098487854004,
            "unit": "ns",
            "range": "± 0.5607456588996104"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 37.24530071020126,
            "unit": "ns",
            "range": "± 0.2838917793952911"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 427.95918893814087,
            "unit": "ns",
            "range": "± 6.575543750574618"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 270.1213501294454,
            "unit": "ns",
            "range": "± 0.7784409413645491"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 31683.652079264324,
            "unit": "ns",
            "range": "± 279.2863672740226"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 725.0237601598104,
            "unit": "ns",
            "range": "± 6.43202746377637"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 138.91878938674927,
            "unit": "ns",
            "range": "± 1.7195160566165932"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 564.2261740366617,
            "unit": "ns",
            "range": "± 15.530600437457373"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 273.454124768575,
            "unit": "ns",
            "range": "± 0.44030779012849963"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 270715.828125,
            "unit": "ns",
            "range": "± 2395.5204944913803"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 4298.9545160929365,
            "unit": "ns",
            "range": "± 87.4712836203867"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 136.7421170870463,
            "unit": "ns",
            "range": "± 2.2087753316725256"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 528.3645407358805,
            "unit": "ns",
            "range": "± 2.9775652775505996"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 273.011266708374,
            "unit": "ns",
            "range": "± 1.058804670336241"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2547481.5377604165,
            "unit": "ns",
            "range": "± 13846.092223989363"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 29571.40199279785,
            "unit": "ns",
            "range": "± 211.51201993200166"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 135.5804946422577,
            "unit": "ns",
            "range": "± 1.802231233939418"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 529.1249027252197,
            "unit": "ns",
            "range": "± 11.840688472312614"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047",
          "message": "Implemented the fixes and regression coverage",
          "timestamp": "2026-06-19T16:58:25+02:00",
          "tree_id": "8f76b30848a0900974d4f94cc10a6d8d6cbcc076",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047"
        },
        "date": 1781881620272,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.22067747885982195,
            "unit": "ns",
            "range": "± 0.001961924482219813"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.35567231973012,
            "unit": "ns",
            "range": "± 0.2190896081397278"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 96.43677518765132,
            "unit": "ns",
            "range": "± 0.2599758681976232"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2495.047369003296,
            "unit": "ns",
            "range": "± 17.35809774889885"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2236.9061241149902,
            "unit": "ns",
            "range": "± 4.555823997642461"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 79.1845588684082,
            "unit": "ns",
            "range": "± 2.033165220638277"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.70146822929382,
            "unit": "ns",
            "range": "± 0.033802019440014784"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2244.1335678100586,
            "unit": "ns",
            "range": "± 2.2542076464533722"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2585.7344512939453,
            "unit": "ns",
            "range": "± 2.9320539211864745"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2438.641606648763,
            "unit": "ns",
            "range": "± 13.162871614398554"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9288.409891764322,
            "unit": "ns",
            "range": "± 71.59258002091254"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2265.8830223083496,
            "unit": "ns",
            "range": "± 6.2907151817391345"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2600.287427266439,
            "unit": "ns",
            "range": "± 2.676598165609307"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6729.713912963867,
            "unit": "ns",
            "range": "± 14.029317230110417"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23358.989059448242,
            "unit": "ns",
            "range": "± 140.49192131987868"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2260.5719680786133,
            "unit": "ns",
            "range": "± 11.472221087890054"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2591.5282847086587,
            "unit": "ns",
            "range": "± 4.1239921965738215"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23384.156575520832,
            "unit": "ns",
            "range": "± 106.82345242622611"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 70860.50313313802,
            "unit": "ns",
            "range": "± 564.8319860154867"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 338.10330390930176,
            "unit": "ns",
            "range": "± 0.9785732316488025"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 548.7877470652262,
            "unit": "ns",
            "range": "± 1.8936121159306922"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.595217773069938,
            "unit": "ns",
            "range": "± 0.04485617664903719"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 253.61696751912436,
            "unit": "ns",
            "range": "± 4.277044566038568"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 38.861903339624405,
            "unit": "ns",
            "range": "± 0.5259938283646111"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 148.8485446770986,
            "unit": "ns",
            "range": "± 0.8662210927825434"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.3500883380572,
            "unit": "ns",
            "range": "± 0.1182093019846713"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 420.7407883008321,
            "unit": "ns",
            "range": "± 14.961806376979297"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 261.1120130221049,
            "unit": "ns",
            "range": "± 0.5018316333074685"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30282.40723164876,
            "unit": "ns",
            "range": "± 78.57405136540817"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 667.182944615682,
            "unit": "ns",
            "range": "± 3.3281264721045307"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 134.41457994778952,
            "unit": "ns",
            "range": "± 2.2826067469375246"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 507.7690626780192,
            "unit": "ns",
            "range": "± 2.952209891731675"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 266.70127932230633,
            "unit": "ns",
            "range": "± 1.1996903638845877"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 259168.12337239584,
            "unit": "ns",
            "range": "± 745.9156721051871"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3810.968859354655,
            "unit": "ns",
            "range": "± 24.054513076472027"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 132.00903689861298,
            "unit": "ns",
            "range": "± 0.7252901356171945"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 505.88640689849854,
            "unit": "ns",
            "range": "± 4.065886094722852"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 270.7987904548645,
            "unit": "ns",
            "range": "± 0.3140367467291431"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2520257.5013020835,
            "unit": "ns",
            "range": "± 86075.05581844733"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 27172.67120361328,
            "unit": "ns",
            "range": "± 171.01805048735568"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 133.5401566028595,
            "unit": "ns",
            "range": "± 1.0911983680275907"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 502.5892499287923,
            "unit": "ns",
            "range": "± 3.904586316438017"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6200531259c7c8066e4f520476472ce14710c076",
          "message": "Implement strict selection for transports, removed WithWorkerTransport",
          "timestamp": "2026-06-21T21:53:07+02:00",
          "tree_id": "c7cf5368e928b7d4cd20ddf2f339c3f7e18a4d48",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6200531259c7c8066e4f520476472ce14710c076"
        },
        "date": 1782072124787,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.2151648961007595,
            "unit": "ns",
            "range": "± 0.008496419193062587"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 70.50902740160625,
            "unit": "ns",
            "range": "± 1.2352146409375768"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 97.43419400850932,
            "unit": "ns",
            "range": "± 1.2578191417649371"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2487.8430112202964,
            "unit": "ns",
            "range": "± 9.034470033438348"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2296.202949523926,
            "unit": "ns",
            "range": "± 44.82388328503412"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 83.03118884563446,
            "unit": "ns",
            "range": "± 0.9646097202490166"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.59850893417994,
            "unit": "ns",
            "range": "± 0.13067778111489298"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2292.050577799479,
            "unit": "ns",
            "range": "± 61.59468929667765"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2618.3368339538574,
            "unit": "ns",
            "range": "± 4.1309024516563255"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2455.568776448568,
            "unit": "ns",
            "range": "± 17.217813418367502"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 14421.162490844727,
            "unit": "ns",
            "range": "± 523.3302626786067"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2273.1578369140625,
            "unit": "ns",
            "range": "± 15.19760722400551"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2634.294916788737,
            "unit": "ns",
            "range": "± 25.39028870526574"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6783.233404795329,
            "unit": "ns",
            "range": "± 6.285778722979237"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 31782.551330566406,
            "unit": "ns",
            "range": "± 965.9430163431451"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2264.389071146647,
            "unit": "ns",
            "range": "± 17.793570355856197"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2627.360538482666,
            "unit": "ns",
            "range": "± 3.473070179896379"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23479.60459391276,
            "unit": "ns",
            "range": "± 210.66088985052917"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 93082.21248372395,
            "unit": "ns",
            "range": "± 1317.504506997174"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 342.35835631688434,
            "unit": "ns",
            "range": "± 1.0406474094465004"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 560.4265384674072,
            "unit": "ns",
            "range": "± 1.2028209142612218"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6102820932865143,
            "unit": "ns",
            "range": "± 0.10267770006782498"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 257.13081630071,
            "unit": "ns",
            "range": "± 1.4643877313103812"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 36.38148674368858,
            "unit": "ns",
            "range": "± 0.30337380913013545"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 93.47649502754211,
            "unit": "ns",
            "range": "± 0.6479171418189001"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.56537422537804,
            "unit": "ns",
            "range": "± 0.41551006548819874"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 111.19209909439087,
            "unit": "ns",
            "range": "± 7.562958466888497"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 287.9329017003377,
            "unit": "ns",
            "range": "± 1.54691314377516"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30564.867685953777,
            "unit": "ns",
            "range": "± 119.6142782620495"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 669.8830750783285,
            "unit": "ns",
            "range": "± 5.723770607763019"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 135.0230314731598,
            "unit": "ns",
            "range": "± 1.5240391727740177"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 498.95863183339435,
            "unit": "ns",
            "range": "± 5.1206820401407835"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 266.55347379048663,
            "unit": "ns",
            "range": "± 0.8358556450251027"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 260844.6951497396,
            "unit": "ns",
            "range": "± 1095.8806989585507"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3871.649232228597,
            "unit": "ns",
            "range": "± 125.45247421569815"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 135.8195480108261,
            "unit": "ns",
            "range": "± 1.5189388797164012"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 519.8229192097982,
            "unit": "ns",
            "range": "± 15.267272369135622"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 272.9261757532756,
            "unit": "ns",
            "range": "± 3.0581783560669082"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2476406.81640625,
            "unit": "ns",
            "range": "± 39247.74673508243"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 27818.188883463543,
            "unit": "ns",
            "range": "± 501.437979196337"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 132.9078299999237,
            "unit": "ns",
            "range": "± 2.3927776772580294"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 497.448114713033,
            "unit": "ns",
            "range": "± 6.148431123051618"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c49198ec3adbfa666c5279ae16ddaec36180fb44",
          "message": "Implemented RabbitMQ transport (missed files)",
          "timestamp": "2026-06-21T22:44:41+02:00",
          "tree_id": "2973c02c0d02631a4fd163030b99b7e9cb1d0a07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c49198ec3adbfa666c5279ae16ddaec36180fb44"
        },
        "date": 1782075215353,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21653580541412035,
            "unit": "ns",
            "range": "± 0.0013332971883215166"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 72.59412274758022,
            "unit": "ns",
            "range": "± 0.4681922330314313"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 95.91840893030167,
            "unit": "ns",
            "range": "± 0.14620494556839547"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 30.208982527256012,
            "unit": "ns",
            "range": "± 0.007681992191027065"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 181.76621822516123,
            "unit": "ns",
            "range": "± 0.9622330702243602"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 29.097899238268536,
            "unit": "ns",
            "range": "± 0.1839890931946913"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 458.90253988901776,
            "unit": "ns",
            "range": "± 13.934982944320595"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2430.641565322876,
            "unit": "ns",
            "range": "± 0.6222375864112216"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2265.333123524984,
            "unit": "ns",
            "range": "± 9.108205487896269"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 73.69007519880931,
            "unit": "ns",
            "range": "± 0.24396345319532334"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 61.333962539831795,
            "unit": "ns",
            "range": "± 0.059897467637215984"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2238.0814348856607,
            "unit": "ns",
            "range": "± 10.151791284354516"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2591.4792861938477,
            "unit": "ns",
            "range": "± 5.09741510821229"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2422.699769337972,
            "unit": "ns",
            "range": "± 9.353399058750316"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9069.644816080729,
            "unit": "ns",
            "range": "± 164.85866016296234"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2251.9540100097656,
            "unit": "ns",
            "range": "± 5.339866303906297"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2566.704605102539,
            "unit": "ns",
            "range": "± 5.148435905966972"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6628.454828898112,
            "unit": "ns",
            "range": "± 3.4988224692400944"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 22832.016479492188,
            "unit": "ns",
            "range": "± 463.80954853342126"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2236.563622792562,
            "unit": "ns",
            "range": "± 23.023006695602856"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2591.69957224528,
            "unit": "ns",
            "range": "± 2.744673842101491"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23392.123774210613,
            "unit": "ns",
            "range": "± 232.1013534594134"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 71722.427734375,
            "unit": "ns",
            "range": "± 295.9118762608171"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 369.0190655390422,
            "unit": "ns",
            "range": "± 0.4053744501924008"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 548.12593237559,
            "unit": "ns",
            "range": "± 1.864160621945472"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5450553211073081,
            "unit": "ns",
            "range": "± 0.00021145343930723838"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 246.0791153907776,
            "unit": "ns",
            "range": "± 1.141599620329783"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 35.95008683204651,
            "unit": "ns",
            "range": "± 0.34116475270491986"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 137.88472509384155,
            "unit": "ns",
            "range": "± 1.260457272387131"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 38.776868999004364,
            "unit": "ns",
            "range": "± 0.19705190636170816"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 408.91856320699054,
            "unit": "ns",
            "range": "± 21.678244403559614"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 259.40796820322674,
            "unit": "ns",
            "range": "± 0.6366681107416793"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29840.192464192707,
            "unit": "ns",
            "range": "± 177.1460469948849"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 638.5565687815348,
            "unit": "ns",
            "range": "± 4.797683474469971"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 128.1616345246633,
            "unit": "ns",
            "range": "± 0.571188664527256"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 506.6200771331787,
            "unit": "ns",
            "range": "± 0.23746274801408515"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 262.6406995455424,
            "unit": "ns",
            "range": "± 0.3470196654905269"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 256328.89029947916,
            "unit": "ns",
            "range": "± 1580.77021474259"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3644.3869654337564,
            "unit": "ns",
            "range": "± 10.642110527302135"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 128.0780476729075,
            "unit": "ns",
            "range": "± 0.32173474972165006"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 482.2864125569661,
            "unit": "ns",
            "range": "± 3.8873098105140897"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 270.0780161221822,
            "unit": "ns",
            "range": "± 0.31291620392934427"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2415250.6614583335,
            "unit": "ns",
            "range": "± 10045.773126641358"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 26410.63474527995,
            "unit": "ns",
            "range": "± 430.0248900226437"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 120.65182209014893,
            "unit": "ns",
            "range": "± 1.1101514804903776"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 484.6807870864868,
            "unit": "ns",
            "range": "± 4.467141069627897"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "686b85a23c07f19ba921f2a1e68c232abd8cb596",
          "message": "Added missed RabbitMQ transport tests",
          "timestamp": "2026-06-21T23:33:43+02:00",
          "tree_id": "e978f3421bf474f0c4b15d8bcb963ef385f57f0a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/686b85a23c07f19ba921f2a1e68c232abd8cb596"
        },
        "date": 1782078164182,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.22186030074954033,
            "unit": "ns",
            "range": "± 0.005680406596375001"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.80440262953441,
            "unit": "ns",
            "range": "± 0.3724728948809236"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 96.89497810602188,
            "unit": "ns",
            "range": "± 0.2191763458896936"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.59556559721629,
            "unit": "ns",
            "range": "± 0.07524927768782833"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 221.9080441792806,
            "unit": "ns",
            "range": "± 1.3192309807149507"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 29.05417247613271,
            "unit": "ns",
            "range": "± 0.029659629854474522"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 430.05515480041504,
            "unit": "ns",
            "range": "± 17.485872943862038"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2448.184387842814,
            "unit": "ns",
            "range": "± 7.3000012351384465"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2262.4575068155923,
            "unit": "ns",
            "range": "± 5.172818755322548"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 75.18694651126862,
            "unit": "ns",
            "range": "± 0.158960674747196"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.1985190709432,
            "unit": "ns",
            "range": "± 0.03526833693144587"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2232.0718256632485,
            "unit": "ns",
            "range": "± 3.753046188782944"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2567.5108846028647,
            "unit": "ns",
            "range": "± 7.368417518682982"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2440.7617098490396,
            "unit": "ns",
            "range": "± 3.93083093480802"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9185.219345092773,
            "unit": "ns",
            "range": "± 69.00244536762696"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2216.33513768514,
            "unit": "ns",
            "range": "± 4.005780878499783"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2587.730973561605,
            "unit": "ns",
            "range": "± 3.438555536626601"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6661.106641133626,
            "unit": "ns",
            "range": "± 12.066440441185296"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23422.7758992513,
            "unit": "ns",
            "range": "± 493.69006571000494"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2233.7303314208984,
            "unit": "ns",
            "range": "± 8.984568229733254"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2576.5150871276855,
            "unit": "ns",
            "range": "± 3.170660731019373"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23295.926340738934,
            "unit": "ns",
            "range": "± 134.83246859402564"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 71414.76733398438,
            "unit": "ns",
            "range": "± 723.739652323047"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 341.5102294286092,
            "unit": "ns",
            "range": "± 1.3594918503189186"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 559.8287878036499,
            "unit": "ns",
            "range": "± 1.4937250862371696"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5433284379541874,
            "unit": "ns",
            "range": "± 0.004671489622807916"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 242.13330427805582,
            "unit": "ns",
            "range": "± 0.8525492409269109"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 35.88922828435898,
            "unit": "ns",
            "range": "± 0.16766810708454702"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 144.25432880719504,
            "unit": "ns",
            "range": "± 0.9310103929752819"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.35072465737661,
            "unit": "ns",
            "range": "± 0.1714023768343145"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 422.6448342005412,
            "unit": "ns",
            "range": "± 21.667403241096668"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 274.2326925595601,
            "unit": "ns",
            "range": "± 1.288500695075623"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30333.784606933594,
            "unit": "ns",
            "range": "± 126.4230194770741"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 665.5481767654419,
            "unit": "ns",
            "range": "± 11.706701546607329"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 130.39733290672302,
            "unit": "ns",
            "range": "± 0.9576688340777363"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 518.5784031550089,
            "unit": "ns",
            "range": "± 15.579092148390409"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 277.7750144004822,
            "unit": "ns",
            "range": "± 0.6103625458051358"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 259729.9933268229,
            "unit": "ns",
            "range": "± 1486.5407783875048"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3716.76469039917,
            "unit": "ns",
            "range": "± 22.813827759552886"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 128.47457162539163,
            "unit": "ns",
            "range": "± 0.5293281214794421"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 486.2040328979492,
            "unit": "ns",
            "range": "± 0.6455970579536119"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 261.14524189631146,
            "unit": "ns",
            "range": "± 1.1564547006765038"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2478592.9505208335,
            "unit": "ns",
            "range": "± 42675.010999911465"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 26876.659108479816,
            "unit": "ns",
            "range": "± 372.17363109578343"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 121.64603169759114,
            "unit": "ns",
            "range": "± 1.9140557638084412"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 508.2135645548503,
            "unit": "ns",
            "range": "± 14.336785301971242"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "37bf44eb46beda36fbd0802457d5b3c60f41af02",
          "message": "Fixed RabbitMQ issues",
          "timestamp": "2026-06-22T00:04:25+02:00",
          "tree_id": "b9335f02d1174db66f8774b231c9e6b2280c2856",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/37bf44eb46beda36fbd0802457d5b3c60f41af02"
        },
        "date": 1782080007574,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.22140893215934435,
            "unit": "ns",
            "range": "± 0.0009448745820418748"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 72.08421516418457,
            "unit": "ns",
            "range": "± 0.5377824879322942"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 93.89625483751297,
            "unit": "ns",
            "range": "± 0.604798766840297"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.710563083489735,
            "unit": "ns",
            "range": "± 0.0314458498929653"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 186.46978640556335,
            "unit": "ns",
            "range": "± 0.30696640610077497"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.009709576765697,
            "unit": "ns",
            "range": "± 0.025653546025301746"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 453.4161467552185,
            "unit": "ns",
            "range": "± 11.326585159344614"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2427.6401068369546,
            "unit": "ns",
            "range": "± 7.202865980159337"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2199.504108428955,
            "unit": "ns",
            "range": "± 3.8780196798405124"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 73.41400329271953,
            "unit": "ns",
            "range": "± 0.44254788889433605"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 57.293729643026985,
            "unit": "ns",
            "range": "± 0.3698153664677902"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2245.2193501790366,
            "unit": "ns",
            "range": "± 11.30042167132"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2553.196952819824,
            "unit": "ns",
            "range": "± 5.774364819442255"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2411.9523544311523,
            "unit": "ns",
            "range": "± 2.425159753021358"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9023.090545654297,
            "unit": "ns",
            "range": "± 44.129862811944484"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2206.6531092325845,
            "unit": "ns",
            "range": "± 7.291098467716972"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2588.495001475016,
            "unit": "ns",
            "range": "± 15.149230490385905"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6635.317738850911,
            "unit": "ns",
            "range": "± 11.01482672414606"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23198.563807169598,
            "unit": "ns",
            "range": "± 361.20567579714333"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2259.9114430745444,
            "unit": "ns",
            "range": "± 1.8021195579823632"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2587.9222424825034,
            "unit": "ns",
            "range": "± 9.511654758221537"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23085.100392659504,
            "unit": "ns",
            "range": "± 71.25718940066612"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 72528.88187662761,
            "unit": "ns",
            "range": "± 1659.8827667340877"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 342.4906749725342,
            "unit": "ns",
            "range": "± 0.6547759477306875"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 542.9121497472128,
            "unit": "ns",
            "range": "± 1.8730511567002246"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5471582276125748,
            "unit": "ns",
            "range": "± 0.006706191918041184"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 243.440438747406,
            "unit": "ns",
            "range": "± 1.139818709339147"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 35.95972468455633,
            "unit": "ns",
            "range": "± 0.2696375291563128"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 148.2433242003123,
            "unit": "ns",
            "range": "± 1.0765724518414528"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 35.704177021980286,
            "unit": "ns",
            "range": "± 0.10737032626103822"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 407.66188796361286,
            "unit": "ns",
            "range": "± 7.22474031505075"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 259.60244671503705,
            "unit": "ns",
            "range": "± 1.015254569614485"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29656.580154418945,
            "unit": "ns",
            "range": "± 61.950435148505775"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 626.1729996999105,
            "unit": "ns",
            "range": "± 3.7798241736639104"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 128.57080356280008,
            "unit": "ns",
            "range": "± 1.0148419318520068"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 509.27277247111004,
            "unit": "ns",
            "range": "± 2.50645931991738"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 279.8711407979329,
            "unit": "ns",
            "range": "± 0.32791346142491434"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 258608.7216796875,
            "unit": "ns",
            "range": "± 816.6885702049242"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3624.506539662679,
            "unit": "ns",
            "range": "± 21.761037750249525"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 127.13394260406494,
            "unit": "ns",
            "range": "± 0.1558898383113446"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 545.7116101582845,
            "unit": "ns",
            "range": "± 3.908571619088983"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 279.6944481531779,
            "unit": "ns",
            "range": "± 0.8486742023635753"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2466619.95703125,
            "unit": "ns",
            "range": "± 6504.608667958343"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 26658.763107299805,
            "unit": "ns",
            "range": "± 264.1401148060684"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 129.40320845444998,
            "unit": "ns",
            "range": "± 0.571816811174001"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 482.0861059824626,
            "unit": "ns",
            "range": "± 5.068469721085458"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "cafc92ff97dac0cce4e301ef5228322f2c979cef",
          "message": "Implemented the recoverable-builder split",
          "timestamp": "2026-06-22T12:25:07+02:00",
          "tree_id": "0274ba7fb878a5231da1a658de8558283e00dfb7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/cafc92ff97dac0cce4e301ef5228322f2c979cef"
        },
        "date": 1782124306215,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21956585720181465,
            "unit": "ns",
            "range": "± 0.002426720669827857"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 84.0494997104009,
            "unit": "ns",
            "range": "± 0.06098295048908791"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 96.8597424030304,
            "unit": "ns",
            "range": "± 0.6173668696416726"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 31.822498659292858,
            "unit": "ns",
            "range": "± 0.015976024474804198"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 187.3712351322174,
            "unit": "ns",
            "range": "± 0.44599074551907597"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.31330840786298,
            "unit": "ns",
            "range": "± 0.438718194068165"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 400.95791419347125,
            "unit": "ns",
            "range": "± 7.927258616209388"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2456.483915964762,
            "unit": "ns",
            "range": "± 10.014644946925511"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2245.280401865641,
            "unit": "ns",
            "range": "± 41.164946987684786"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 75.47283679246902,
            "unit": "ns",
            "range": "± 1.2823293063138728"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.18058058619499,
            "unit": "ns",
            "range": "± 0.05766173503646183"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2254.751926422119,
            "unit": "ns",
            "range": "± 6.16914888041252"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2565.6277554829917,
            "unit": "ns",
            "range": "± 2.4124181701937424"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2463.065516153971,
            "unit": "ns",
            "range": "± 27.182425721649007"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9109.358774820963,
            "unit": "ns",
            "range": "± 104.08971153443123"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2284.1045049031577,
            "unit": "ns",
            "range": "± 29.25012871583931"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2586.756763458252,
            "unit": "ns",
            "range": "± 4.490332600602654"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6791.841987609863,
            "unit": "ns",
            "range": "± 53.653888369111286"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23132.685419718426,
            "unit": "ns",
            "range": "± 249.29028457833323"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2253.0526796976724,
            "unit": "ns",
            "range": "± 3.7447253641166323"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2587.4675229390464,
            "unit": "ns",
            "range": "± 6.956307491720862"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23702.295939127605,
            "unit": "ns",
            "range": "± 192.29042486615825"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 72001.42936197917,
            "unit": "ns",
            "range": "± 286.07112879426165"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 340.7355008125305,
            "unit": "ns",
            "range": "± 1.6685029248238572"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 562.8296777407328,
            "unit": "ns",
            "range": "± 19.0667016157381"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.544373416652282,
            "unit": "ns",
            "range": "± 0.0012779458057946703"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 239.75823783874512,
            "unit": "ns",
            "range": "± 0.6381876405747811"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 37.462733030319214,
            "unit": "ns",
            "range": "± 0.46575577693301"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 168.2806735833486,
            "unit": "ns",
            "range": "± 0.738785861873572"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 37.12925193707148,
            "unit": "ns",
            "range": "± 0.3415126753972194"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 413.598318417867,
            "unit": "ns",
            "range": "± 12.824849306364905"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 274.1478870709737,
            "unit": "ns",
            "range": "± 2.4368446395211567"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29679.16424560547,
            "unit": "ns",
            "range": "± 192.20959095692209"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 634.9734773635864,
            "unit": "ns",
            "range": "± 7.980419756345744"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 141.76283037662506,
            "unit": "ns",
            "range": "± 5.064131516874825"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 513.9688857396444,
            "unit": "ns",
            "range": "± 2.6258761310713266"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 275.8424792289734,
            "unit": "ns",
            "range": "± 2.559385252902706"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 268890.79215494794,
            "unit": "ns",
            "range": "± 1516.451539381738"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3970.130195617676,
            "unit": "ns",
            "range": "± 4.462324366086222"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 128.9688387711843,
            "unit": "ns",
            "range": "± 1.2022407670124784"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 496.8276710510254,
            "unit": "ns",
            "range": "± 11.351879940008823"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 271.38013855616254,
            "unit": "ns",
            "range": "± 1.5535923967614274"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2438665.1184895835,
            "unit": "ns",
            "range": "± 11510.598396508283"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 27578.656036376953,
            "unit": "ns",
            "range": "± 918.0522772958345"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 134.0952136516571,
            "unit": "ns",
            "range": "± 0.47108435495629347"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 493.62679799397785,
            "unit": "ns",
            "range": "± 3.357357147721294"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "4c47254f985de03b7e3448345a6c611fc96d9287",
          "message": "more test coverage",
          "timestamp": "2026-06-22T13:12:19+02:00",
          "tree_id": "4e7d907efc034ba9796d2580da02bc18c268bf07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/4c47254f985de03b7e3448345a6c611fc96d9287"
        },
        "date": 1782127278575,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.23526383688052496,
            "unit": "ns",
            "range": "± 0.021815742076078542"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 76.72729853789012,
            "unit": "ns",
            "range": "± 1.6494488166210874"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 109.08168882131577,
            "unit": "ns",
            "range": "± 0.3964603580429675"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.252049763997395,
            "unit": "ns",
            "range": "± 0.029607418541613882"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 180.3807408809662,
            "unit": "ns",
            "range": "± 0.1725720649871705"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.023256371418636,
            "unit": "ns",
            "range": "± 0.02083632922535537"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 414.16705894470215,
            "unit": "ns",
            "range": "± 19.662610893774556"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2534.918066660563,
            "unit": "ns",
            "range": "± 5.5118798505958795"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2255.869597117106,
            "unit": "ns",
            "range": "± 6.7819977094936155"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 77.18306008974712,
            "unit": "ns",
            "range": "± 0.74797207419124"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.679963767528534,
            "unit": "ns",
            "range": "± 0.05091391705157489"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2259.1104316711426,
            "unit": "ns",
            "range": "± 1.5090125365217983"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2657.760644276937,
            "unit": "ns",
            "range": "± 4.885453248452339"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2451.641554514567,
            "unit": "ns",
            "range": "± 10.804094884399504"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9219.458437601725,
            "unit": "ns",
            "range": "± 22.307605783006036"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2271.558666229248,
            "unit": "ns",
            "range": "± 2.0338906001083537"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2618.169246673584,
            "unit": "ns",
            "range": "± 4.040923685265976"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6930.570742289226,
            "unit": "ns",
            "range": "± 11.103348080118161"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23835.06185913086,
            "unit": "ns",
            "range": "± 529.5532602351338"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2252.900491078695,
            "unit": "ns",
            "range": "± 10.87167298355282"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2683.286553700765,
            "unit": "ns",
            "range": "± 9.50691183266246"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 24125.471603393555,
            "unit": "ns",
            "range": "± 103.61521152711283"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 73411.96618652344,
            "unit": "ns",
            "range": "± 1186.4552516240803"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 353.62214914957684,
            "unit": "ns",
            "range": "± 2.0390531354472876"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 547.9892403284708,
            "unit": "ns",
            "range": "± 1.2345167305784066"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5461459135015805,
            "unit": "ns",
            "range": "± 0.0032712283019361087"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 239.7754661242167,
            "unit": "ns",
            "range": "± 0.9898477589474179"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 36.6115500330925,
            "unit": "ns",
            "range": "± 0.4795038168901267"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 139.06797369321188,
            "unit": "ns",
            "range": "± 1.0608915832838277"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.45046307643255,
            "unit": "ns",
            "range": "± 0.10335963461703637"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 417.16829919815063,
            "unit": "ns",
            "range": "± 15.504698693328706"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 267.25184631347656,
            "unit": "ns",
            "range": "± 0.9070826836436721"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30369.62798055013,
            "unit": "ns",
            "range": "± 71.4846138745518"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 651.1702286402384,
            "unit": "ns",
            "range": "± 4.213698878462948"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 135.76487267017365,
            "unit": "ns",
            "range": "± 1.3543528042633772"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 519.2370802561442,
            "unit": "ns",
            "range": "± 3.448673489752467"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 266.0601065158844,
            "unit": "ns",
            "range": "± 1.087134768550962"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 263986.984375,
            "unit": "ns",
            "range": "± 936.770422702299"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 4012.897041320801,
            "unit": "ns",
            "range": "± 161.22445950913453"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 131.91452181339264,
            "unit": "ns",
            "range": "± 0.5498692412549216"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 502.6748695373535,
            "unit": "ns",
            "range": "± 5.342981806458722"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 275.94767729441327,
            "unit": "ns",
            "range": "± 2.248290814981779"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2475807.5559895835,
            "unit": "ns",
            "range": "± 11031.12632234439"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 28026.23710123698,
            "unit": "ns",
            "range": "± 306.901167086349"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 134.29050306479135,
            "unit": "ns",
            "range": "± 1.7501049493333378"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 516.8975324630737,
            "unit": "ns",
            "range": "± 2.270067984825151"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "442bd30053d373021f390964218348a9512de5b2",
          "message": "Implemented the Redis transport end to end (missed files)",
          "timestamp": "2026-06-22T18:54:51+02:00",
          "tree_id": "cf38fd5a6b520278ac71920f6f329eb3ce9501f0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/442bd30053d373021f390964218348a9512de5b2"
        },
        "date": 1782147883177,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0,
            "unit": "ns",
            "range": "± 0"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.15194906791051,
            "unit": "ns",
            "range": "± 0.7858389266570662"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 99.90493299563725,
            "unit": "ns",
            "range": "± 0.08807097266279924"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 28.64849599202474,
            "unit": "ns",
            "range": "± 0.04919081633912059"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 160.53983553250632,
            "unit": "ns",
            "range": "± 0.5368320269729997"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 28.052208522955578,
            "unit": "ns",
            "range": "± 0.032277295096832546"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 412.54980246225995,
            "unit": "ns",
            "range": "± 12.98528326628856"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2704.6084747314453,
            "unit": "ns",
            "range": "± 4.112936486214762"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2533.0596605936685,
            "unit": "ns",
            "range": "± 3.0521755386784486"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 57.994700491428375,
            "unit": "ns",
            "range": "± 0.1812409095615754"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 210.04129123687744,
            "unit": "ns",
            "range": "± 3.1267670518498236"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 56.89440278212229,
            "unit": "ns",
            "range": "± 0.26088921068099524"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 538.7431109746298,
            "unit": "ns",
            "range": "± 8.844536183137585"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 83.485622326533,
            "unit": "ns",
            "range": "± 1.3321662611350054"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 60.12908907731374,
            "unit": "ns",
            "range": "± 0.23650767839046474"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2456.239175160726,
            "unit": "ns",
            "range": "± 11.573092181688537"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2921.4309120178223,
            "unit": "ns",
            "range": "± 1.1711549269755475"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2706.3668162027993,
            "unit": "ns",
            "range": "± 7.265710970179004"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9230.027013142904,
            "unit": "ns",
            "range": "± 127.31214812021453"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2484.2531763712564,
            "unit": "ns",
            "range": "± 4.337123468479545"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2871.6140530904136,
            "unit": "ns",
            "range": "± 5.561224826637547"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 7475.84779103597,
            "unit": "ns",
            "range": "± 16.03386772663876"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23055.645619710285,
            "unit": "ns",
            "range": "± 131.4942936495771"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2505.464185078939,
            "unit": "ns",
            "range": "± 15.283556170708763"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2899.0096015930176,
            "unit": "ns",
            "range": "± 3.3830996830279285"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 25460.577962239582,
            "unit": "ns",
            "range": "± 103.83781085041026"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 72989.66617838542,
            "unit": "ns",
            "range": "± 1263.6230280497398"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 298.57342354456586,
            "unit": "ns",
            "range": "± 0.2806391261446435"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 523.0507357915243,
            "unit": "ns",
            "range": "± 1.0679065068482125"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.672657339523236,
            "unit": "ns",
            "range": "± 0.002942043537840379"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 234.4577604929606,
            "unit": "ns",
            "range": "± 0.16253951530734537"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 32.71833105882009,
            "unit": "ns",
            "range": "± 0.25042895376014035"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 166.83121164639792,
            "unit": "ns",
            "range": "± 0.9310841267419"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 32.7226851383845,
            "unit": "ns",
            "range": "± 0.13454775682268064"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 425.4295997619629,
            "unit": "ns",
            "range": "± 15.38632475344028"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 242.0282095273336,
            "unit": "ns",
            "range": "± 0.1365804898767837"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 30492.353591918945,
            "unit": "ns",
            "range": "± 403.35210180448456"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 573.4011093775431,
            "unit": "ns",
            "range": "± 19.92160858028273"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 129.2614860534668,
            "unit": "ns",
            "range": "± 0.8783666872583014"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 470.4227571487427,
            "unit": "ns",
            "range": "± 1.01999260538486"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 246.6852881113688,
            "unit": "ns",
            "range": "± 2.9448362028335704"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 276468.7073567708,
            "unit": "ns",
            "range": "± 533.8131015003607"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3724.6854260762534,
            "unit": "ns",
            "range": "± 120.32932287754176"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 121.54402716954549,
            "unit": "ns",
            "range": "± 0.5041585269917241"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 475.079070409139,
            "unit": "ns",
            "range": "± 8.05305325644163"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 240.98471403121948,
            "unit": "ns",
            "range": "± 0.6425057012433799"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2395598.044921875,
            "unit": "ns",
            "range": "± 13355.15087925193"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 26155.090108235676,
            "unit": "ns",
            "range": "± 316.83169715600263"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 120.11842783292134,
            "unit": "ns",
            "range": "± 0.29305769287626493"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 469.3727448781331,
            "unit": "ns",
            "range": "± 0.965684065344209"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5a70579754ca2f51207cb9c6fd9f4681d0d529cf",
          "message": "Redis transport fixes",
          "timestamp": "2026-06-22T19:26:49+02:00",
          "tree_id": "0aff004e6924fb9cb656f2c8e583e4ae6b4a19d9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5a70579754ca2f51207cb9c6fd9f4681d0d529cf"
        },
        "date": 1782149651408,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.2621168817083041,
            "unit": "ns",
            "range": "± 0.02630117350712694"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 57.67083623011907,
            "unit": "ns",
            "range": "± 0.5783572330077937"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 81.68833539883296,
            "unit": "ns",
            "range": "± 0.3925584342430485"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 21.482267886400223,
            "unit": "ns",
            "range": "± 0.0034679803414167524"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 128.57899260520935,
            "unit": "ns",
            "range": "± 1.1475105525914626"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 22.020429988702137,
            "unit": "ns",
            "range": "± 0.010746496470325925"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 341.73922872543335,
            "unit": "ns",
            "range": "± 22.33738087120998"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2166.7071940104165,
            "unit": "ns",
            "range": "± 17.866421601553622"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 1953.762616475423,
            "unit": "ns",
            "range": "± 3.3455634327450885"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 42.280510465304054,
            "unit": "ns",
            "range": "± 0.2620759062153294"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 165.05537470181784,
            "unit": "ns",
            "range": "± 35.03939899147843"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 44.14977324008942,
            "unit": "ns",
            "range": "± 0.02867392562995955"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 425.96627680460614,
            "unit": "ns",
            "range": "± 2.2862334523115124"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 65.17226155598958,
            "unit": "ns",
            "range": "± 0.5542260640204498"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 45.55658559997877,
            "unit": "ns",
            "range": "± 0.02941272104145692"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 1937.7167326609294,
            "unit": "ns",
            "range": "± 1.4920611240946084"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2275.7864735921225,
            "unit": "ns",
            "range": "± 7.556523900252858"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2174.9028231302896,
            "unit": "ns",
            "range": "± 35.782828199787126"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 7533.33512878418,
            "unit": "ns",
            "range": "± 67.89524884407224"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 1897.4971758524578,
            "unit": "ns",
            "range": "± 7.903613947367111"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2246.3845087687173,
            "unit": "ns",
            "range": "± 7.330945288561611"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 5813.689709981282,
            "unit": "ns",
            "range": "± 59.59699582760058"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 17485.496322631836,
            "unit": "ns",
            "range": "± 197.66578967318407"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2002.0887056986492,
            "unit": "ns",
            "range": "± 32.01864130282453"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2265.980136871338,
            "unit": "ns",
            "range": "± 8.669567289238172"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 20162.551005045574,
            "unit": "ns",
            "range": "± 46.73475605519851"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 58014.49256388346,
            "unit": "ns",
            "range": "± 919.1483644478529"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 224.82123974959055,
            "unit": "ns",
            "range": "± 0.6480878553364612"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 409.7361207008362,
            "unit": "ns",
            "range": "± 0.6530779266890899"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.298510159055392,
            "unit": "ns",
            "range": "± 0.0027559457754798154"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 178.04158663749695,
            "unit": "ns",
            "range": "± 0.3526665913758967"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 25.93112783630689,
            "unit": "ns",
            "range": "± 0.09734701131088437"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 114.77230836947759,
            "unit": "ns",
            "range": "± 0.6320214538530019"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 27.155669540166855,
            "unit": "ns",
            "range": "± 0.034796940477174874"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 281.51810725529987,
            "unit": "ns",
            "range": "± 4.883519774185976"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 185.749049504598,
            "unit": "ns",
            "range": "± 0.3143068484113218"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 22915.327570597332,
            "unit": "ns",
            "range": "± 338.8794354013437"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 431.9702337582906,
            "unit": "ns",
            "range": "± 1.386747468317436"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 93.91971776882808,
            "unit": "ns",
            "range": "± 0.09554792462894104"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 419.38864278793335,
            "unit": "ns",
            "range": "± 2.282201295510587"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 183.26788981755575,
            "unit": "ns",
            "range": "± 0.6323502493952116"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 191057.63452148438,
            "unit": "ns",
            "range": "± 317.3796243264037"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 2697.8493436177573,
            "unit": "ns",
            "range": "± 29.03178595055141"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 93.658056418101,
            "unit": "ns",
            "range": "± 0.06901543941588155"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 367.2959926923116,
            "unit": "ns",
            "range": "± 7.90093236170947"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 192.4790178934733,
            "unit": "ns",
            "range": "± 1.3632008390515706"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 1856401.318359375,
            "unit": "ns",
            "range": "± 5344.9966320997855"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 19934.5298055013,
            "unit": "ns",
            "range": "± 52.00178592394722"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 94.08787673711777,
            "unit": "ns",
            "range": "± 0.06907128049719544"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 382.2604049046834,
            "unit": "ns",
            "range": "± 4.626815322705726"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "676e73cbdca8c6975ee7b9b6d61220e8659b4642",
          "message": "package bump",
          "timestamp": "2026-06-22T19:38:14+02:00",
          "tree_id": "651f1d6e0d6e1c83abe1535756905c38d937d452",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/676e73cbdca8c6975ee7b9b6d61220e8659b4642"
        },
        "date": 1782150338766,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0,
            "unit": "ns",
            "range": "± 0"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.3618512948354,
            "unit": "ns",
            "range": "± 0.26459571478237137"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 99.20785061518352,
            "unit": "ns",
            "range": "± 0.2671388589653198"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 27.697079598903656,
            "unit": "ns",
            "range": "± 0.023899720569936778"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 165.9401175181071,
            "unit": "ns",
            "range": "± 1.2567393557720201"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 28.612403293450672,
            "unit": "ns",
            "range": "± 0.5486640546688367"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 472.46196667353314,
            "unit": "ns",
            "range": "± 14.800018226735308"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2790.5627772013345,
            "unit": "ns",
            "range": "± 2.8478502736287052"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2528.6876538594565,
            "unit": "ns",
            "range": "± 6.827225305166976"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 59.34572768211365,
            "unit": "ns",
            "range": "± 0.347597034471404"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 261.2838764190674,
            "unit": "ns",
            "range": "± 4.686711532999289"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 58.49254419406255,
            "unit": "ns",
            "range": "± 1.8739801178564532"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 564.4123992919922,
            "unit": "ns",
            "range": "± 29.71155405185091"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 84.73156297206879,
            "unit": "ns",
            "range": "± 0.6143654435517941"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 58.102078914642334,
            "unit": "ns",
            "range": "± 0.15127836297526007"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2598.188279469808,
            "unit": "ns",
            "range": "± 14.819487596489429"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2875.8896115620933,
            "unit": "ns",
            "range": "± 9.594633343822764"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2707.619010925293,
            "unit": "ns",
            "range": "± 0.30518137077934765"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9314.079442342123,
            "unit": "ns",
            "range": "± 105.10399565605653"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2494.177719116211,
            "unit": "ns",
            "range": "± 10.729138029536102"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2908.2557106018066,
            "unit": "ns",
            "range": "± 3.846382274532307"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 7534.3547770182295,
            "unit": "ns",
            "range": "± 7.182853679623305"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23100.0678507487,
            "unit": "ns",
            "range": "± 153.96281472052345"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2509.386672973633,
            "unit": "ns",
            "range": "± 8.569570263286867"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2924.3072942097983,
            "unit": "ns",
            "range": "± 2.8211077132440376"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 26493.36114501953,
            "unit": "ns",
            "range": "± 244.8540037245167"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 73905.36092122395,
            "unit": "ns",
            "range": "± 1199.0481725539491"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 301.5139576594035,
            "unit": "ns",
            "range": "± 1.9254624019766002"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 522.904496828715,
            "unit": "ns",
            "range": "± 1.3166909777833706"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.6704672711590927,
            "unit": "ns",
            "range": "± 0.0007182159178144553"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 238.03973166147867,
            "unit": "ns",
            "range": "± 0.9229264817785198"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 32.68972723682722,
            "unit": "ns",
            "range": "± 0.1988951796519727"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 157.08748014767966,
            "unit": "ns",
            "range": "± 2.4231717908770083"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 32.306913594404854,
            "unit": "ns",
            "range": "± 0.12528340572678037"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 444.6726214090983,
            "unit": "ns",
            "range": "± 16.81651594730943"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 240.20343828201294,
            "unit": "ns",
            "range": "± 1.254083951907032"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29172.077799479168,
            "unit": "ns",
            "range": "± 75.3778197973345"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 577.5505224863688,
            "unit": "ns",
            "range": "± 9.229858199284926"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 121.84795753161113,
            "unit": "ns",
            "range": "± 0.15910358658090226"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 496.68329334259033,
            "unit": "ns",
            "range": "± 6.6914674700859065"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 239.48717085520425,
            "unit": "ns",
            "range": "± 0.22936688177996115"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 247602.90584309897,
            "unit": "ns",
            "range": "± 724.1779873275916"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3747.600752512614,
            "unit": "ns",
            "range": "± 54.2401422643525"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 121.85474801063538,
            "unit": "ns",
            "range": "± 0.5149069375333694"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 474.5366946856181,
            "unit": "ns",
            "range": "± 3.6546707596798753"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 266.6230454444885,
            "unit": "ns",
            "range": "± 0.32740868165193254"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2443395.5950520835,
            "unit": "ns",
            "range": "± 14289.859670704322"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 28076.791381835938,
            "unit": "ns",
            "range": "± 1579.9063903453728"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 123.24837462107341,
            "unit": "ns",
            "range": "± 0.5753451360375017"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 481.29027366638184,
            "unit": "ns",
            "range": "± 2.0850452834076836"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a619da4e5271a0eb7adb52cc27fac70e8b3bf35d",
          "message": "Implemented NATS channel and transport",
          "timestamp": "2026-06-23T10:20:25+02:00",
          "tree_id": "7df45f29e08480bf8ccf543a91c7de47105bb6dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a619da4e5271a0eb7adb52cc27fac70e8b3bf35d"
        },
        "date": 1782203311756,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.4539511948823929,
            "unit": "ns",
            "range": "± 0.1857772132690481"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 56.2550844947497,
            "unit": "ns",
            "range": "± 0.37296431253673173"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 78.0010574857394,
            "unit": "ns",
            "range": "± 0.8213685759692069"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 21.379816005627315,
            "unit": "ns",
            "range": "± 0.006446880977221276"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 152.615438302358,
            "unit": "ns",
            "range": "± 0.3696223657153294"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 22.032945056756336,
            "unit": "ns",
            "range": "± 0.0032940517262437454"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 344.2891058921814,
            "unit": "ns",
            "range": "± 11.007058335757598"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2132.705140431722,
            "unit": "ns",
            "range": "± 46.40907321696057"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 1907.48619556427,
            "unit": "ns",
            "range": "± 7.839326862633076"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 42.76415499051412,
            "unit": "ns",
            "range": "± 0.03676140368749471"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 187.31924311319986,
            "unit": "ns",
            "range": "± 3.8834299249864084"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 43.06153351068497,
            "unit": "ns",
            "range": "± 0.0071328342661180905"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 455.23567295074463,
            "unit": "ns",
            "range": "± 8.784476275038068"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 21.341650466124218,
            "unit": "ns",
            "range": "± 0.16077682027123683"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 198.74972295761108,
            "unit": "ns",
            "range": "± 1.9408747735955922"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 19.971806248029072,
            "unit": "ns",
            "range": "± 0.03717279517259783"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 493.8284972508748,
            "unit": "ns",
            "range": "± 17.566673508876804"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 64.33725517988205,
            "unit": "ns",
            "range": "± 0.44599286364101365"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 45.57942432165146,
            "unit": "ns",
            "range": "± 0.00413338878146119"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 1907.2220141092937,
            "unit": "ns",
            "range": "± 2.4939605447283144"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2257.5986239115396,
            "unit": "ns",
            "range": "± 8.52079286186065"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2116.2737922668457,
            "unit": "ns",
            "range": "± 2.1488170302131904"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 7610.26708984375,
            "unit": "ns",
            "range": "± 77.38749695746665"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 1909.7841720581055,
            "unit": "ns",
            "range": "± 1.6607017205155448"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2252.878952026367,
            "unit": "ns",
            "range": "± 28.509803081778113"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 5832.430280049642,
            "unit": "ns",
            "range": "± 5.4071022426197795"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 17785.189325968426,
            "unit": "ns",
            "range": "± 200.70600015098145"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 1921.3511060078938,
            "unit": "ns",
            "range": "± 9.81682426982622"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2259.5293604532876,
            "unit": "ns",
            "range": "± 4.68712399793935"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 20248.086980183918,
            "unit": "ns",
            "range": "± 125.47834116325275"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 57304.96689860026,
            "unit": "ns",
            "range": "± 121.2130868965249"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 228.60697944959006,
            "unit": "ns",
            "range": "± 0.42889658935431835"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 402.59386889139813,
            "unit": "ns",
            "range": "± 0.4345278711527191"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.2965620172520478,
            "unit": "ns",
            "range": "± 0.0007532911968688327"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 178.6237197717031,
            "unit": "ns",
            "range": "± 0.22518176778796423"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 25.093823273976643,
            "unit": "ns",
            "range": "± 0.025263598329673485"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 146.6167550086975,
            "unit": "ns",
            "range": "± 1.05837458240516"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 25.26141397158305,
            "unit": "ns",
            "range": "± 0.025869262470529734"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 290.8116617202759,
            "unit": "ns",
            "range": "± 22.633975021253978"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 184.386368672053,
            "unit": "ns",
            "range": "± 0.5585808840739573"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 22605.53712972005,
            "unit": "ns",
            "range": "± 14.68560429905471"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 424.55392567316693,
            "unit": "ns",
            "range": "± 4.215438927862975"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 95.1194799542427,
            "unit": "ns",
            "range": "± 0.20920514522531378"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 373.7332094510396,
            "unit": "ns",
            "range": "± 3.3096567285667216"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 184.41750041643778,
            "unit": "ns",
            "range": "± 2.2762639205940314"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 190977.12394205728,
            "unit": "ns",
            "range": "± 220.45476450048986"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 2752.0613327026367,
            "unit": "ns",
            "range": "± 22.591155282548307"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 94.5975132783254,
            "unit": "ns",
            "range": "± 0.3159257718046528"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 366.13198534647626,
            "unit": "ns",
            "range": "± 5.375464556174154"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 185.25890811284384,
            "unit": "ns",
            "range": "± 0.682784934835567"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 1839027.5768229167,
            "unit": "ns",
            "range": "± 6960.546103775173"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 20042.23156229655,
            "unit": "ns",
            "range": "± 41.53132658107058"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 95.38147775332133,
            "unit": "ns",
            "range": "± 1.3325018211558177"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 369.4401610692342,
            "unit": "ns",
            "range": "± 1.293312523774859"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "78706e145b78cb241e547ddcdb73c8913a921b9b",
          "message": "Fixed NATS issues, added tests",
          "timestamp": "2026-06-23T11:24:19+02:00",
          "tree_id": "d0993e96aaccecda7fc145e3f8a483a1aadca718",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/78706e145b78cb241e547ddcdb73c8913a921b9b"
        },
        "date": 1782207130005,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21936206643780073,
            "unit": "ns",
            "range": "± 0.0035854488810784335"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 74.37166047096252,
            "unit": "ns",
            "range": "± 0.6142380109573081"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 94.58037118117015,
            "unit": "ns",
            "range": "± 0.29546393558196066"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.343643506368,
            "unit": "ns",
            "range": "± 0.014388567287554879"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 215.23012375831604,
            "unit": "ns",
            "range": "± 0.4946027423148638"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.194725692272186,
            "unit": "ns",
            "range": "± 0.36782737495588597"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 457.46026706695557,
            "unit": "ns",
            "range": "± 10.127717270980327"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2458.311509450277,
            "unit": "ns",
            "range": "± 3.3235674484661692"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2253.613857269287,
            "unit": "ns",
            "range": "± 24.54573158365615"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 56.641623636086784,
            "unit": "ns",
            "range": "± 0.2518976843666811"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 188.98846562703451,
            "unit": "ns",
            "range": "± 4.1237580390267"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 58.18740568558375,
            "unit": "ns",
            "range": "± 0.09465741786717151"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 489.8408209482829,
            "unit": "ns",
            "range": "± 11.597973308480055"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 24.704054882129032,
            "unit": "ns",
            "range": "± 0.030219347401981064"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 239.14042472839355,
            "unit": "ns",
            "range": "± 0.46428030379069624"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 22.871958702802658,
            "unit": "ns",
            "range": "± 0.5017370032338094"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 627.595869064331,
            "unit": "ns",
            "range": "± 19.19930130702343"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 72.94261467456818,
            "unit": "ns",
            "range": "± 0.40185113757306384"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.37510186433792,
            "unit": "ns",
            "range": "± 0.13487117536068627"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2213.2311515808105,
            "unit": "ns",
            "range": "± 5.791250750912355"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2576.239865620931,
            "unit": "ns",
            "range": "± 5.9882369420234465"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2416.886563618978,
            "unit": "ns",
            "range": "± 19.50519940304694"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 8986.794153849283,
            "unit": "ns",
            "range": "± 119.43152536803409"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2220.330523173014,
            "unit": "ns",
            "range": "± 4.148901550648357"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2587.3710492451987,
            "unit": "ns",
            "range": "± 7.882256069738209"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6722.130869547526,
            "unit": "ns",
            "range": "± 41.33791788256634"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23489.704752604168,
            "unit": "ns",
            "range": "± 355.53925538455627"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2297.6234550476074,
            "unit": "ns",
            "range": "± 5.8672541405695835"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2588.7148780822754,
            "unit": "ns",
            "range": "± 2.375836918570542"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23576.23501078288,
            "unit": "ns",
            "range": "± 215.35570514916608"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74605.3984375,
            "unit": "ns",
            "range": "± 3243.6001736858593"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 346.9744499524434,
            "unit": "ns",
            "range": "± 0.5679041133700979"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 564.4222455024719,
            "unit": "ns",
            "range": "± 0.6450559080174767"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5471778238813083,
            "unit": "ns",
            "range": "± 0.0004122786258945401"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 243.50307909647623,
            "unit": "ns",
            "range": "± 1.2006052561921354"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 34.70361925164858,
            "unit": "ns",
            "range": "± 0.09198944621691584"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 139.98871080080667,
            "unit": "ns",
            "range": "± 1.5647616435817544"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 35.23145873347918,
            "unit": "ns",
            "range": "± 0.31965390797864995"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 348.3208969434102,
            "unit": "ns",
            "range": "± 9.367032256017641"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 259.94209480285645,
            "unit": "ns",
            "range": "± 1.4975337914812117"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29297.954701741535,
            "unit": "ns",
            "range": "± 47.322687437969236"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 645.2872959772745,
            "unit": "ns",
            "range": "± 4.93677054315382"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 129.51413774490356,
            "unit": "ns",
            "range": "± 0.6199611645847672"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 474.23190275828046,
            "unit": "ns",
            "range": "± 3.8693108292485103"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 265.26902564366657,
            "unit": "ns",
            "range": "± 2.9245066920902207"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 251854.314453125,
            "unit": "ns",
            "range": "± 960.0279597578087"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3659.9426460266113,
            "unit": "ns",
            "range": "± 82.43002709204693"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 126.89994978904724,
            "unit": "ns",
            "range": "± 0.1045939557100908"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 479.18702602386475,
            "unit": "ns",
            "range": "± 1.511388284185796"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 264.1377520561218,
            "unit": "ns",
            "range": "± 1.1171639362809855"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2520775.8893229165,
            "unit": "ns",
            "range": "± 13727.176442646902"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 25623.396697998047,
            "unit": "ns",
            "range": "± 288.11679701029016"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 125.5669931570689,
            "unit": "ns",
            "range": "± 0.16442875868062362"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 472.787491162618,
            "unit": "ns",
            "range": "± 1.138762619769022"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "21a784c103f1c1c705195a58da20adc6425d007c",
          "message": "optimization",
          "timestamp": "2026-06-23T16:27:39+02:00",
          "tree_id": "7fbeb262c123e61793cda0685899f2f4bc34523e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/21a784c103f1c1c705195a58da20adc6425d007c"
        },
        "date": 1782225512229,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21946757286787033,
            "unit": "ns",
            "range": "± 0.004257923537495875"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 64.95230762163798,
            "unit": "ns",
            "range": "± 1.135328053233115"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 94.44371008872986,
            "unit": "ns",
            "range": "± 0.17220365580531447"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 30.070550282796223,
            "unit": "ns",
            "range": "± 0.029083591801810173"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 206.19470850626627,
            "unit": "ns",
            "range": "± 7.689223822681766"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 29.58624068895976,
            "unit": "ns",
            "range": "± 0.03770772869546612"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 423.72042385737103,
            "unit": "ns",
            "range": "± 10.085124009440575"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2401.182549794515,
            "unit": "ns",
            "range": "± 16.51353554121892"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2189.742561340332,
            "unit": "ns",
            "range": "± 1.7249059935189417"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 57.53758587439855,
            "unit": "ns",
            "range": "± 0.09828937049461041"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 195.11591561635336,
            "unit": "ns",
            "range": "± 5.866282111908273"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 56.95734894275665,
            "unit": "ns",
            "range": "± 0.15509821866964243"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 532.9275801976522,
            "unit": "ns",
            "range": "± 1.86980942187534"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 22.586444238821667,
            "unit": "ns",
            "range": "± 0.012438075438436296"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 190.40427573521933,
            "unit": "ns",
            "range": "± 8.196250982829365"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 22.264402727286022,
            "unit": "ns",
            "range": "± 0.06968564817445427"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 650.612948735555,
            "unit": "ns",
            "range": "± 47.7993110455596"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 71.5694785118103,
            "unit": "ns",
            "range": "± 0.2986565456648568"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 55.49776816368103,
            "unit": "ns",
            "range": "± 0.13081036294987572"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2172.3245582580566,
            "unit": "ns",
            "range": "± 7.148188080111348"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2590.7643127441406,
            "unit": "ns",
            "range": "± 8.644224848234193"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2372.575392405192,
            "unit": "ns",
            "range": "± 5.35711098970088"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 8990.824289957682,
            "unit": "ns",
            "range": "± 7.847830178198947"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2185.6189181009927,
            "unit": "ns",
            "range": "± 7.113505753749108"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2542.667995452881,
            "unit": "ns",
            "range": "± 8.377210288457626"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6587.35671488444,
            "unit": "ns",
            "range": "± 17.33790091897716"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 22566.354385375977,
            "unit": "ns",
            "range": "± 372.8069750115544"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2172.7824630737305,
            "unit": "ns",
            "range": "± 5.266496740267671"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2530.1646575927734,
            "unit": "ns",
            "range": "± 24.696521615261833"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23124.871810913086,
            "unit": "ns",
            "range": "± 119.36760172956426"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 69247.16349283855,
            "unit": "ns",
            "range": "± 827.2412541914075"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 334.2905481656392,
            "unit": "ns",
            "range": "± 0.27989588481123046"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 538.1474272410074,
            "unit": "ns",
            "range": "± 1.4927701856780342"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.564573800812165,
            "unit": "ns",
            "range": "± 0.033545962064774365"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 242.36065006256104,
            "unit": "ns",
            "range": "± 0.38310973575250273"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 34.92929069201151,
            "unit": "ns",
            "range": "± 0.17451830287918538"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 162.61673219998679,
            "unit": "ns",
            "range": "± 0.6670160133474695"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.96793285012245,
            "unit": "ns",
            "range": "± 0.3146377100611433"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 357.3626578648885,
            "unit": "ns",
            "range": "± 10.231531837131136"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 262.93675367037457,
            "unit": "ns",
            "range": "± 1.0227279286841735"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29400.50099690755,
            "unit": "ns",
            "range": "± 26.431985667889418"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 602.008145014445,
            "unit": "ns",
            "range": "± 0.22708962350097883"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 124.68647885322571,
            "unit": "ns",
            "range": "± 0.2587396774619728"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 466.8215684890747,
            "unit": "ns",
            "range": "± 2.055907849683795"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 273.95874309539795,
            "unit": "ns",
            "range": "± 0.811464876305266"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 251663.43310546875,
            "unit": "ns",
            "range": "± 751.5416990774579"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3519.4016456604004,
            "unit": "ns",
            "range": "± 20.537021956956593"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 125.07268551985423,
            "unit": "ns",
            "range": "± 0.44204037087904235"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 462.9431324005127,
            "unit": "ns",
            "range": "± 0.1905521358515298"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 254.61442359288534,
            "unit": "ns",
            "range": "± 3.7940112134047665"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2373179.03125,
            "unit": "ns",
            "range": "± 10119.835858629478"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 25406.413533528645,
            "unit": "ns",
            "range": "± 71.70211838164248"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 126.0121085246404,
            "unit": "ns",
            "range": "± 0.6386703404381887"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 480.3505271275838,
            "unit": "ns",
            "range": "± 1.0621698492391907"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7be0409f6dd42811feab06ca36700a5651c6f75f",
          "message": "refactor: hardening and optimizations",
          "timestamp": "2026-06-24T16:06:48+02:00",
          "tree_id": "013ba14d18b3283a0e58dc77fc4bc1f420a710d5",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7be0409f6dd42811feab06ca36700a5651c6f75f"
        },
        "date": 1782310484770,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21699895213047662,
            "unit": "ns",
            "range": "± 0.006152464237086324"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 72.65461001793544,
            "unit": "ns",
            "range": "± 0.9286757087405296"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 96.7173531850179,
            "unit": "ns",
            "range": "± 0.8643844329777528"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.890314241250355,
            "unit": "ns",
            "range": "± 0.05543284329134771"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 238.9915931224823,
            "unit": "ns",
            "range": "± 22.41873930171968"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.43503012259801,
            "unit": "ns",
            "range": "± 0.012662005267948603"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 434.6643163363139,
            "unit": "ns",
            "range": "± 9.594849940260277"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2499.561922709147,
            "unit": "ns",
            "range": "± 12.510573675020089"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2279.236857096354,
            "unit": "ns",
            "range": "± 2.0115120073729424"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 58.29546356201172,
            "unit": "ns",
            "range": "± 0.4693046775149672"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 143.34854221343994,
            "unit": "ns",
            "range": "± 1.0598305707700684"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 63.56029750903448,
            "unit": "ns",
            "range": "± 0.6277862582742796"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 506.76008653640747,
            "unit": "ns",
            "range": "± 15.660941934836368"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 23.679319421450298,
            "unit": "ns",
            "range": "± 0.17278148319641856"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 210.17693090438843,
            "unit": "ns",
            "range": "± 3.7048407794928533"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 23.558759222428005,
            "unit": "ns",
            "range": "± 0.16779159014872275"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 663.2712825139364,
            "unit": "ns",
            "range": "± 28.41443726887568"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 79.22158203522365,
            "unit": "ns",
            "range": "± 1.0059442685742432"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 71.78370662530263,
            "unit": "ns",
            "range": "± 0.6273914644579455"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2261.8455225626626,
            "unit": "ns",
            "range": "± 7.157829808538549"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2676.1491470336914,
            "unit": "ns",
            "range": "± 17.451401799330235"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2457.976266225179,
            "unit": "ns",
            "range": "± 14.885620045584176"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9196.000473022461,
            "unit": "ns",
            "range": "± 40.00997605020262"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2271.592919667562,
            "unit": "ns",
            "range": "± 8.522082900705504"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2652.6376597086587,
            "unit": "ns",
            "range": "± 25.23948942362673"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6861.7651926676435,
            "unit": "ns",
            "range": "± 102.48914186907558"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23388.993723551434,
            "unit": "ns",
            "range": "± 169.7830010538501"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2251.7698453267417,
            "unit": "ns",
            "range": "± 1.2554159185076939"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2628.278689066569,
            "unit": "ns",
            "range": "± 7.208333932584766"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23696.287404378254,
            "unit": "ns",
            "range": "± 314.82021032434903"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 72436.5853881836,
            "unit": "ns",
            "range": "± 1845.1323855942533"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 372.8529655138652,
            "unit": "ns",
            "range": "± 0.6941565089610491"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 633.994039217631,
            "unit": "ns",
            "range": "± 5.374629482013149"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5480887827773888,
            "unit": "ns",
            "range": "± 0.005941498303466016"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 241.21956141789755,
            "unit": "ns",
            "range": "± 0.8070828719952253"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 40.235324730475746,
            "unit": "ns",
            "range": "± 0.479633473057492"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 141.64407642682394,
            "unit": "ns",
            "range": "± 0.719674824236234"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 37.27258414030075,
            "unit": "ns",
            "range": "± 0.23262772901461456"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 355.4351569811503,
            "unit": "ns",
            "range": "± 14.213669977169676"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 262.77488978703815,
            "unit": "ns",
            "range": "± 1.2606246014852156"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29783.32062784831,
            "unit": "ns",
            "range": "± 86.39466798977236"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 585.6862802505493,
            "unit": "ns",
            "range": "± 8.981541057867323"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 127.7845238049825,
            "unit": "ns",
            "range": "± 0.662656211030119"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 494.2801628112793,
            "unit": "ns",
            "range": "± 4.010251184333265"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 260.90471919377643,
            "unit": "ns",
            "range": "± 1.8636103232813694"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 256987.4747721354,
            "unit": "ns",
            "range": "± 1290.0420589490268"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3708.3348960876465,
            "unit": "ns",
            "range": "± 101.59193595957377"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 128.73086015383402,
            "unit": "ns",
            "range": "± 1.303398558528296"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 487.0311228434245,
            "unit": "ns",
            "range": "± 2.007339635961085"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 255.39078839619955,
            "unit": "ns",
            "range": "± 0.259625221369286"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2477309.0052083335,
            "unit": "ns",
            "range": "± 6503.601431578575"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 26461.769755045574,
            "unit": "ns",
            "range": "± 202.69582207918194"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 119.04395830631256,
            "unit": "ns",
            "range": "± 0.19612849986736217"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 493.06629212697345,
            "unit": "ns",
            "range": "± 2.6809168864471786"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "67a76d645b0d5cd5117227fd338420064fcc2d6c",
          "message": "fixed loadtest apphost logging error",
          "timestamp": "2026-06-24T16:43:48+02:00",
          "tree_id": "0ffcee11ee0bd58b08b2ed5d84cf646530dd8bfe",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/67a76d645b0d5cd5117227fd338420064fcc2d6c"
        },
        "date": 1782312688009,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21671565746267638,
            "unit": "ns",
            "range": "± 0.0021295266677490246"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 71.83908073107402,
            "unit": "ns",
            "range": "± 0.9204343204179295"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 95.9813381632169,
            "unit": "ns",
            "range": "± 0.4224049777187119"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.988565921783447,
            "unit": "ns",
            "range": "± 0.024170582930658924"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 171.86234029134116,
            "unit": "ns",
            "range": "± 2.4220614752316907"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 29.813958207766216,
            "unit": "ns",
            "range": "± 0.07603793685422917"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 412.2209647496541,
            "unit": "ns",
            "range": "± 12.518167699837365"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2456.867949167887,
            "unit": "ns",
            "range": "± 2.466284353322031"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2241.496664683024,
            "unit": "ns",
            "range": "± 6.381934134478247"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 57.787746926148735,
            "unit": "ns",
            "range": "± 0.31346089448895315"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 163.07986203829446,
            "unit": "ns",
            "range": "± 1.5707961826000931"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 60.14583214124044,
            "unit": "ns",
            "range": "± 0.785142385552449"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 520.8894521395365,
            "unit": "ns",
            "range": "± 39.32277436019071"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 23.001034031311672,
            "unit": "ns",
            "range": "± 0.11937180352832404"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 204.6807463169098,
            "unit": "ns",
            "range": "± 1.1115171738915886"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 22.546393275260925,
            "unit": "ns",
            "range": "± 0.058933198587249956"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 633.0529947280884,
            "unit": "ns",
            "range": "± 29.935450940968188"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 75.0916696190834,
            "unit": "ns",
            "range": "± 0.6837929201542996"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 69.81181724866231,
            "unit": "ns",
            "range": "± 0.12365798140175885"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2206.934711456299,
            "unit": "ns",
            "range": "± 4.4440212699486965"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2614.174124399821,
            "unit": "ns",
            "range": "± 4.762880215795021"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2444.916717529297,
            "unit": "ns",
            "range": "± 11.295716690249519"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9308.008926391602,
            "unit": "ns",
            "range": "± 60.51361420014042"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2238.025922139486,
            "unit": "ns",
            "range": "± 7.051579566326534"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2628.796901702881,
            "unit": "ns",
            "range": "± 35.82490219168436"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6655.893653869629,
            "unit": "ns",
            "range": "± 19.57518532218658"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23572.159556070965,
            "unit": "ns",
            "range": "± 177.61874558026042"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2242.9665654500327,
            "unit": "ns",
            "range": "± 5.930182050782145"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2618.7117627461753,
            "unit": "ns",
            "range": "± 9.415530932510677"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23558.77507527669,
            "unit": "ns",
            "range": "± 165.48620678210355"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 72073.88256835938,
            "unit": "ns",
            "range": "± 168.23720285720634"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 359.6905698776245,
            "unit": "ns",
            "range": "± 4.664901091345629"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 605.0328181584676,
            "unit": "ns",
            "range": "± 1.1053753985734802"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.542663571735223,
            "unit": "ns",
            "range": "± 0.0007736599150060681"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 247.80400371551514,
            "unit": "ns",
            "range": "± 1.8958920998085944"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 35.60179481903712,
            "unit": "ns",
            "range": "± 0.14675338203604088"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 145.1815711657206,
            "unit": "ns",
            "range": "± 0.22958686492480587"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.25505624214808,
            "unit": "ns",
            "range": "± 0.1258430129388333"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 360.21145979563397,
            "unit": "ns",
            "range": "± 4.03082586638042"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 254.59842586517334,
            "unit": "ns",
            "range": "± 1.8194497808078816"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 29932.066970825195,
            "unit": "ns",
            "range": "± 34.61652059112807"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 623.0398639043173,
            "unit": "ns",
            "range": "± 3.115961261555322"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 128.9420771598816,
            "unit": "ns",
            "range": "± 1.1824707839558524"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 482.6894184748332,
            "unit": "ns",
            "range": "± 3.5006874501030487"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 278.2199894587199,
            "unit": "ns",
            "range": "± 0.9516271277445717"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 255720.27294921875,
            "unit": "ns",
            "range": "± 1086.1862754842311"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 3684.3539644877114,
            "unit": "ns",
            "range": "± 52.18621658232795"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 126.20407732327779,
            "unit": "ns",
            "range": "± 0.3227854130366742"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 488.54702885945636,
            "unit": "ns",
            "range": "± 2.032474043344249"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 261.5121947924296,
            "unit": "ns",
            "range": "± 0.903428352817391"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 2416268.7213541665,
            "unit": "ns",
            "range": "± 5941.928901234586"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 25587.651967366535,
            "unit": "ns",
            "range": "± 324.35643714955063"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 128.3655530611674,
            "unit": "ns",
            "range": "± 1.0266551019348447"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 487.14208666483563,
            "unit": "ns",
            "range": "± 3.0554413513762166"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd",
          "message": "Made lost-subscriber recovery coherent for multiple recoverable waiters on one correlation id",
          "timestamp": "2026-06-29T10:36:44+02:00",
          "tree_id": "666953ca1f47ae96e2377b15a22bac3f0303adcd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd"
        },
        "date": 1782722819832,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.28904147694508237,
            "unit": "ns",
            "range": "± 0.041464748924433185"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 81.2024696667989,
            "unit": "ns",
            "range": "± 0.7779909574846368"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 95.51038869222005,
            "unit": "ns",
            "range": "± 0.4087729660931362"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.317520399888355,
            "unit": "ns",
            "range": "± 0.01730020220784247"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 242.1469734509786,
            "unit": "ns",
            "range": "± 2.9781048170515727"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 29.76605447133382,
            "unit": "ns",
            "range": "± 0.024575978130261016"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 429.4603355725606,
            "unit": "ns",
            "range": "± 10.161112351795307"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2543.756881713867,
            "unit": "ns",
            "range": "± 7.618518718099321"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2288.75048828125,
            "unit": "ns",
            "range": "± 4.654954782952907"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 57.548807779947914,
            "unit": "ns",
            "range": "± 1.0694169489027798"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 144.8755443096161,
            "unit": "ns",
            "range": "± 3.1101738527931615"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 58.05180195967356,
            "unit": "ns",
            "range": "± 0.5138561772633545"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 484.06904284159344,
            "unit": "ns",
            "range": "± 15.367862081454014"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 23.630958924690884,
            "unit": "ns",
            "range": "± 0.4172656934175703"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 185.75510470072427,
            "unit": "ns",
            "range": "± 6.016877109164561"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 23.695485333601635,
            "unit": "ns",
            "range": "± 0.36656189459025684"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 608.4867451985677,
            "unit": "ns",
            "range": "± 10.81596443065893"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 77.84443904956181,
            "unit": "ns",
            "range": "± 0.6984144278222943"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 69.71196375290553,
            "unit": "ns",
            "range": "± 0.36477305381344477"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2320.5397249857583,
            "unit": "ns",
            "range": "± 17.302665037829303"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2702.1830266316733,
            "unit": "ns",
            "range": "± 23.476494673764808"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2522.3507512410483,
            "unit": "ns",
            "range": "± 19.394539349154698"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9228.850143432617,
            "unit": "ns",
            "range": "± 143.7573055042415"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2325.5826886494956,
            "unit": "ns",
            "range": "± 26.394580566270456"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2658.130138397217,
            "unit": "ns",
            "range": "± 2.6686742527008835"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6911.877833048503,
            "unit": "ns",
            "range": "± 12.114500305101284"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 24442.611689249676,
            "unit": "ns",
            "range": "± 283.90987124017255"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2375.6532033284507,
            "unit": "ns",
            "range": "± 3.4464962602775215"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2724.95277150472,
            "unit": "ns",
            "range": "± 6.977103613318078"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 24572.337376912434,
            "unit": "ns",
            "range": "± 109.62726011713221"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74121.59859212239,
            "unit": "ns",
            "range": "± 547.8094290037182"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 358.9849106470744,
            "unit": "ns",
            "range": "± 3.0497424653676446"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 613.2110363642374,
            "unit": "ns",
            "range": "± 0.604113726874114"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5453989865879219,
            "unit": "ns",
            "range": "± 0.0008712981483890995"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 252.97132889429727,
            "unit": "ns",
            "range": "± 3.873144609610872"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 34.966624001661934,
            "unit": "ns",
            "range": "± 0.23914438777411767"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 150.3351912498474,
            "unit": "ns",
            "range": "± 0.30632099690368636"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.270474165678024,
            "unit": "ns",
            "range": "± 0.7235220954380538"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 384.8492995897929,
            "unit": "ns",
            "range": "± 7.650043344957339"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 1137.4678840637207,
            "unit": "ns",
            "range": "± 6.136995707925577"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 111054.2138671875,
            "unit": "ns",
            "range": "± 573.7418286156296"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 3005.3882776896157,
            "unit": "ns",
            "range": "± 57.97122903187782"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 133.5257725318273,
            "unit": "ns",
            "range": "± 1.4583755095148445"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 484.4491659800212,
            "unit": "ns",
            "range": "± 3.282288158142408"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 1146.349084218343,
            "unit": "ns",
            "range": "± 0.717695634853749"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 933586.7158203125,
            "unit": "ns",
            "range": "± 1400.72036260249"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 23735.577758789062,
            "unit": "ns",
            "range": "± 718.4692134583443"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 119.9036934375763,
            "unit": "ns",
            "range": "± 0.9797392507282816"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 488.68610286712646,
            "unit": "ns",
            "range": "± 7.858001025797491"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 1128.5002218882244,
            "unit": "ns",
            "range": "± 1.5843552041430242"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 7930255.635416667,
            "unit": "ns",
            "range": "± 98590.21314752859"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 335163.87581380206,
            "unit": "ns",
            "range": "± 2970.322421686029"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 129.23047637939453,
            "unit": "ns",
            "range": "± 2.0443806335291903"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 517.7844765981039,
            "unit": "ns",
            "range": "± 4.960324095244677"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5ff27f3ad385c13043a4bb6d7baabc612d9e9c53",
          "message": "Fixed exception stack traces are reset",
          "timestamp": "2026-06-29T10:52:54+02:00",
          "tree_id": "71de51782d1d8ea53d8f6f74204f34cc90f51af8",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5ff27f3ad385c13043a4bb6d7baabc612d9e9c53"
        },
        "date": 1782723644211,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.2188491684695085,
            "unit": "ns",
            "range": "± 0.004643985524585862"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 72.09663186470668,
            "unit": "ns",
            "range": "± 0.038766466025488985"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 93.18116460243861,
            "unit": "ns",
            "range": "± 0.007208376340485829"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 30.33867832024892,
            "unit": "ns",
            "range": "± 0.09132131017398672"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 213.78493603070578,
            "unit": "ns",
            "range": "± 0.6416323067831435"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 29.90303095181783,
            "unit": "ns",
            "range": "± 0.018134933384176325"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 438.16380707422894,
            "unit": "ns",
            "range": "± 17.21447863577507"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2463.2856419881186,
            "unit": "ns",
            "range": "± 8.397704105313384"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2274.46199798584,
            "unit": "ns",
            "range": "± 4.172921148625082"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 60.021714886029564,
            "unit": "ns",
            "range": "± 1.6529916696051516"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 163.97650639216104,
            "unit": "ns",
            "range": "± 3.0766350289254034"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 64.64881193637848,
            "unit": "ns",
            "range": "± 1.4077038805220181"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 529.2278312047323,
            "unit": "ns",
            "range": "± 31.86110160602591"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 23.716119527816772,
            "unit": "ns",
            "range": "± 0.07040619003708297"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 215.24873916308084,
            "unit": "ns",
            "range": "± 5.167689528693155"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 23.284005691607792,
            "unit": "ns",
            "range": "± 0.25871957196958045"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 610.9051974614462,
            "unit": "ns",
            "range": "± 52.81127290790533"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 76.34081049760182,
            "unit": "ns",
            "range": "± 0.41439078489021164"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 72.64050608873367,
            "unit": "ns",
            "range": "± 1.4129828638494555"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2282.0359586079917,
            "unit": "ns",
            "range": "± 2.409996090724347"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2650.632651011149,
            "unit": "ns",
            "range": "± 7.20619089387049"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2449.550755818685,
            "unit": "ns",
            "range": "± 3.4199036025795655"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9211.520523071289,
            "unit": "ns",
            "range": "± 36.73165604535712"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2268.064264933268,
            "unit": "ns",
            "range": "± 2.2953804481833484"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2658.7926228841147,
            "unit": "ns",
            "range": "± 4.015079286752998"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 6824.135559082031,
            "unit": "ns",
            "range": "± 5.3770383485688455"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23597.010798136394,
            "unit": "ns",
            "range": "± 118.44464256703318"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2279.004591623942,
            "unit": "ns",
            "range": "± 6.164038219152534"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2631.80641301473,
            "unit": "ns",
            "range": "± 3.7861680219244263"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 23971.70179748535,
            "unit": "ns",
            "range": "± 46.4560878623376"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 73845.72770182292,
            "unit": "ns",
            "range": "± 1258.5873671802701"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 364.00599336624146,
            "unit": "ns",
            "range": "± 0.8402164278169924"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 665.6208391189575,
            "unit": "ns",
            "range": "± 4.039283624431981"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5432472911973794,
            "unit": "ns",
            "range": "± 0.0018581360285547896"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 268.71185843149823,
            "unit": "ns",
            "range": "± 1.0882368334269397"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 35.74861868222555,
            "unit": "ns",
            "range": "± 0.3228219379663743"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 157.3219834168752,
            "unit": "ns",
            "range": "± 0.6583422160374979"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 76.4880624016126,
            "unit": "ns",
            "range": "± 0.5034723309361537"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 411.0471167564392,
            "unit": "ns",
            "range": "± 9.390221950861841"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 1113.5273501078289,
            "unit": "ns",
            "range": "± 6.856288999961113"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 112426.80155436198,
            "unit": "ns",
            "range": "± 240.44580176811928"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 2948.51926167806,
            "unit": "ns",
            "range": "± 42.445528866960636"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 126.0039308865865,
            "unit": "ns",
            "range": "± 0.9426639443963529"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 492.757939974467,
            "unit": "ns",
            "range": "± 1.9165705175312773"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 1106.6306896209717,
            "unit": "ns",
            "range": "± 2.860530797648081"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 900776.53515625,
            "unit": "ns",
            "range": "± 3345.251223986346"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 22480.516438802082,
            "unit": "ns",
            "range": "± 346.96430116772245"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 124.33058571815491,
            "unit": "ns",
            "range": "± 0.5200768750068293"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 474.25098991394043,
            "unit": "ns",
            "range": "± 1.018308189537419"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 1111.870734532674,
            "unit": "ns",
            "range": "± 3.621204424953573"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 7842581.893229167,
            "unit": "ns",
            "range": "± 57436.614339588086"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 333275.8834635417,
            "unit": "ns",
            "range": "± 1931.0392575886412"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 128.14343599478403,
            "unit": "ns",
            "range": "± 1.0072212152987445"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 497.30116176605225,
            "unit": "ns",
            "range": "± 3.1084058883108616"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7fa908b6b83b2dde7e515f43228a49722069fd49",
          "message": "In-Memory performance fix",
          "timestamp": "2026-06-29T11:23:34+02:00",
          "tree_id": "6915c4f995598ebc2c12837e4143aedddc85a38b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7fa908b6b83b2dde7e515f43228a49722069fd49"
        },
        "date": 1782725505965,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.24406553556521735,
            "unit": "ns",
            "range": "± 0.04395348154721984"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 64.823435485363,
            "unit": "ns",
            "range": "± 0.4037339292390148"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 92.32687429587047,
            "unit": "ns",
            "range": "± 0.33679689412415387"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.71353805065155,
            "unit": "ns",
            "range": "± 0.01011409567591773"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 235.54603656133017,
            "unit": "ns",
            "range": "± 1.6680048986795484"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.00397006670634,
            "unit": "ns",
            "range": "± 0.009576804012645063"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 422.04842948913574,
            "unit": "ns",
            "range": "± 4.0788248696407505"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2513.501209259033,
            "unit": "ns",
            "range": "± 4.909603167615256"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2311.0271937052407,
            "unit": "ns",
            "range": "± 7.167661415891612"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 57.86248254776001,
            "unit": "ns",
            "range": "± 0.8518070570628266"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 239.7980641523997,
            "unit": "ns",
            "range": "± 2.911028046201921"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 60.970840553442635,
            "unit": "ns",
            "range": "± 1.17034389048898"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 577.7713289260864,
            "unit": "ns",
            "range": "± 31.39960872433633"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 23.010057896375656,
            "unit": "ns",
            "range": "± 0.10114301776975396"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 223.80250279108682,
            "unit": "ns",
            "range": "± 6.529852176701537"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 22.41450176636378,
            "unit": "ns",
            "range": "± 0.10258250023500179"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 651.8304421106974,
            "unit": "ns",
            "range": "± 15.325907534975201"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 77.30651372671127,
            "unit": "ns",
            "range": "± 1.0364322852025292"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 70.02293407917023,
            "unit": "ns",
            "range": "± 0.0838972892055334"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2289.925364176432,
            "unit": "ns",
            "range": "± 5.514169389945894"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2660.1590868631997,
            "unit": "ns",
            "range": "± 7.123661953858076"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2478.0215924580893,
            "unit": "ns",
            "range": "± 6.219116925213181"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9184.62717183431,
            "unit": "ns",
            "range": "± 216.06387886141434"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2299.2487716674805,
            "unit": "ns",
            "range": "± 39.659895745731085"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2687.609074910482,
            "unit": "ns",
            "range": "± 47.22340702656512"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 7235.713165283203,
            "unit": "ns",
            "range": "± 63.91284346410879"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23556.292236328125,
            "unit": "ns",
            "range": "± 442.12219299930507"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2283.3687337239585,
            "unit": "ns",
            "range": "± 0.9067849945076657"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2677.828900655111,
            "unit": "ns",
            "range": "± 6.527298299690883"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 26108.239969889324,
            "unit": "ns",
            "range": "± 278.31347934424946"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 74930.55411783855,
            "unit": "ns",
            "range": "± 666.1236496716147"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 376.6742065747579,
            "unit": "ns",
            "range": "± 0.5725763700449599"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 618.3193759918213,
            "unit": "ns",
            "range": "± 8.222651539102447"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5448826253414154,
            "unit": "ns",
            "range": "± 0.004225511559021263"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 252.91888427734375,
            "unit": "ns",
            "range": "± 1.00936309654866"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 35.143144100904465,
            "unit": "ns",
            "range": "± 0.16983760688452176"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 167.20464706420898,
            "unit": "ns",
            "range": "± 0.9288378866881629"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 34.94032233953476,
            "unit": "ns",
            "range": "± 0.14106738094451318"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 401.7643632888794,
            "unit": "ns",
            "range": "± 15.564996276397892"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 927.9609435399374,
            "unit": "ns",
            "range": "± 1.7343828899513944"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 112006.66206868489,
            "unit": "ns",
            "range": "± 996.0922287621669"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 2014.2057825724285,
            "unit": "ns",
            "range": "± 2.285477718082834"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 128.34836792945862,
            "unit": "ns",
            "range": "± 0.18814155430773055"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 513.7952289581299,
            "unit": "ns",
            "range": "± 1.5191551492251136"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 920.2805671691895,
            "unit": "ns",
            "range": "± 2.92579509505542"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 917570.4202473959,
            "unit": "ns",
            "range": "± 1195.9185936763658"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 17874.09373474121,
            "unit": "ns",
            "range": "± 107.56418988224101"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 127.94773713747661,
            "unit": "ns",
            "range": "± 1.6887771297968197"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 470.8938862482707,
            "unit": "ns",
            "range": "± 2.9619366293316522"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 937.4233976999918,
            "unit": "ns",
            "range": "± 6.786300265673545"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 7807560.002604167,
            "unit": "ns",
            "range": "± 18822.09327361771"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 285124.96761067706,
            "unit": "ns",
            "range": "± 961.0716937359421"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 126.77604961395264,
            "unit": "ns",
            "range": "± 1.5053100268906237"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 481.59299914042157,
            "unit": "ns",
            "range": "± 0.6571845515293354"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d",
          "message": "upgrade libs",
          "timestamp": "2026-06-29T11:35:35+02:00",
          "tree_id": "e0664c0bccd14d40756a5d087f5bc6ac66a281e6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d"
        },
        "date": 1782726219483,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_NoPropagators",
            "value": 0.21744339913129807,
            "unit": "ns",
            "range": "± 0.0015579173315602287"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Capture_TwoPropagators",
            "value": 70.52229418357213,
            "unit": "ns",
            "range": "± 0.21192469083648838"
          },
          {
            "name": "AsyncResponse.Benchmarks.ContextPropagationBenchmarks.Restore_TwoPropagators",
            "value": 95.79898353417714,
            "unit": "ns",
            "range": "± 0.4028686560934728"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 29.902761826912563,
            "unit": "ns",
            "range": "± 0.017660124506857378"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 224.97132198015848,
            "unit": "ns",
            "range": "± 0.19565440880761328"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 30.17012184858322,
            "unit": "ns",
            "range": "± 0.03713611273359332"
          },
          {
            "name": "AsyncResponse.Benchmarks.RabbitMqAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 468.55483897527057,
            "unit": "ns",
            "range": "± 10.61314108051744"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaBuilder",
            "value": 2456.912167867025,
            "unit": "ns",
            "range": "± 1.6143924454862453"
          },
          {
            "name": "AsyncResponse.Benchmarks.ChannelBenchmarks.RoundTrip_ViaSubscriber",
            "value": 2252.7029749552407,
            "unit": "ns",
            "range": "± 13.317773835828557"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 61.8891806602478,
            "unit": "ns",
            "range": "± 1.4673949360537677"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 152.60135515530905,
            "unit": "ns",
            "range": "± 1.2648261411528585"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 59.69379578034083,
            "unit": "ns",
            "range": "± 0.27702005498680865"
          },
          {
            "name": "AsyncResponse.Benchmarks.RedisAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 521.2335950533549,
            "unit": "ns",
            "range": "± 12.026021845260377"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 22.95046555995941,
            "unit": "ns",
            "range": "± 0.14964351065466952"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 1)",
            "value": 220.67734535535178,
            "unit": "ns",
            "range": "± 7.212787465793468"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 23.78185151020686,
            "unit": "ns",
            "range": "± 0.4430143941314006"
          },
          {
            "name": "AsyncResponse.Benchmarks.NatsAckDispatchBenchmarks.AckAfterReceive_Callback(BackgroundWorkers: 8)",
            "value": 678.502508799235,
            "unit": "ns",
            "range": "± 15.113222585450663"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ExpressionToReflectionCall",
            "value": 76.8313998579979,
            "unit": "ns",
            "range": "± 0.7517796466457195"
          },
          {
            "name": "AsyncResponse.Benchmarks.CallbackBenchmarks.ReflectionInvoke",
            "value": 70.65956185261409,
            "unit": "ns",
            "range": "± 0.04600646680801324"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 1)",
            "value": 2251.3921979268393,
            "unit": "ns",
            "range": "± 17.913948618198518"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 1)",
            "value": 2638.576446533203,
            "unit": "ns",
            "range": "± 4.588863222901492"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 1)",
            "value": 2456.895804087321,
            "unit": "ns",
            "range": "± 20.919149838710997"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 1)",
            "value": 9095.759081522623,
            "unit": "ns",
            "range": "± 29.256188286241716"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 4)",
            "value": 2245.4740867614746,
            "unit": "ns",
            "range": "± 3.189853764574709"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 4)",
            "value": 2625.275494893392,
            "unit": "ns",
            "range": "± 6.790243869960355"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 4)",
            "value": 7161.320411682129,
            "unit": "ns",
            "range": "± 85.22038420237575"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 4)",
            "value": 23378.631540934246,
            "unit": "ns",
            "range": "± 199.00686116368078"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_RoundTrip(Fanout: 16)",
            "value": 2264.4110972086587,
            "unit": "ns",
            "range": "± 1.7598530698260144"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.RawIngress_RoundTrip(Fanout: 16)",
            "value": 2611.5277112325034,
            "unit": "ns",
            "range": "± 8.387174839376396"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.TypedPublisher_Fanout(Fanout: 16)",
            "value": 25593.154368082684,
            "unit": "ns",
            "range": "± 64.92870873995771"
          },
          {
            "name": "AsyncResponse.Benchmarks.IngressBenchmarks.Exception_Fanout(Fanout: 16)",
            "value": 75788.33520507812,
            "unit": "ns",
            "range": "± 1610.2445841337485"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Serialize",
            "value": 361.29334195454913,
            "unit": "ns",
            "range": "± 1.1448366805391157"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Envelope_Deserialize",
            "value": 602.2337779998779,
            "unit": "ns",
            "range": "± 0.8873244843527127"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_TypedPayload",
            "value": 1.5502457556625207,
            "unit": "ns",
            "range": "± 0.0035865500284507413"
          },
          {
            "name": "AsyncResponse.Benchmarks.SerializationBenchmarks.Classify_RawJson",
            "value": 243.8339522679647,
            "unit": "ns",
            "range": "± 0.7667457310653115"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 1)",
            "value": 38.06574010848999,
            "unit": "ns",
            "range": "± 0.21263028308363205"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 1)",
            "value": 153.20384454727173,
            "unit": "ns",
            "range": "± 0.9992530277187759"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterHandlerCompletes_Callback(BackgroundWorkers: 8)",
            "value": 36.81849912802378,
            "unit": "ns",
            "range": "± 0.16722427918968377"
          },
          {
            "name": "AsyncResponse.Benchmarks.GooglePubSubAckDispatchBenchmarks.AckAfterEnqueue_Callback(BackgroundWorkers: 8)",
            "value": 352.1811363697052,
            "unit": "ns",
            "range": "± 10.166462031670877"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 128)",
            "value": 930.5810747146606,
            "unit": "ns",
            "range": "± 1.2022209894719336"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 128)",
            "value": 110503.349609375,
            "unit": "ns",
            "range": "± 183.43182025171419"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 128)",
            "value": 2163.892911275228,
            "unit": "ns",
            "range": "± 14.170073079860147"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 128)",
            "value": 135.94311332702637,
            "unit": "ns",
            "range": "± 0.8755670361535467"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 128)",
            "value": 496.3366943995158,
            "unit": "ns",
            "range": "± 6.338116212711785"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 1024)",
            "value": 935.9693228403727,
            "unit": "ns",
            "range": "± 0.7861088550627943"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 1024)",
            "value": 924227.8131510416,
            "unit": "ns",
            "range": "± 2716.121926928834"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 1024)",
            "value": 18295.778905232746,
            "unit": "ns",
            "range": "± 523.5392615359624"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 1024)",
            "value": 124.1548752784729,
            "unit": "ns",
            "range": "± 1.5143784411753938"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 1024)",
            "value": 509.97976303100586,
            "unit": "ns",
            "range": "± 15.459189968384907"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_SaveGetDelete(Entries: 8192)",
            "value": 941.0993922551473,
            "unit": "ns",
            "range": "± 0.3238983575622436"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.InMemoryStore_Scan(Entries: 8192)",
            "value": 7990543.864583333,
            "unit": "ns",
            "range": "± 73096.29320444076"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.Watchdog_Evaluate_MixedSnapshot(Entries: 8192)",
            "value": 280948.56982421875,
            "unit": "ns",
            "range": "± 1013.061901429954"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Healthy(Entries: 8192)",
            "value": 130.42705766359964,
            "unit": "ns",
            "range": "± 0.7174488617538018"
          },
          {
            "name": "AsyncResponse.Benchmarks.RecoveryBenchmarks.HealthCheck_Evaluate_Degraded(Entries: 8192)",
            "value": 501.80726591746014,
            "unit": "ns",
            "range": "± 3.069619323193548"
          }
        ]
      }
    ],
    "AsyncResponse Stress - throughput": [
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "336277848e28b1e7d6850920480786681c7f088c",
          "message": "added workflow yml to track performance",
          "timestamp": "2026-06-16T22:03:55+02:00",
          "tree_id": "e47c587b1108eb0a00844a032f8a52d7d22e63c2",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/336277848e28b1e7d6850920480786681c7f088c"
        },
        "date": 1781641102893,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 66875.6038867031,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 34577.500973356655,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 32262.995550739335,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 69911.7462080568,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "22035f2a553c97a781d1704f559d5e08c2930f5b",
          "message": "performance fix",
          "timestamp": "2026-06-16T23:11:02+02:00",
          "tree_id": "f890deb570c7a8d0e6511f623cf7b288fcf08580",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/22035f2a553c97a781d1704f559d5e08c2930f5b"
        },
        "date": 1781644398902,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 85317.94243154771,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 45951.19431749151,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 46936.413363360116,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 136898.68621068817,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ffbb46cb8b4713afec451b9a02dc3de9585069dd",
          "message": "added NBomber load test",
          "timestamp": "2026-06-17T01:31:48+02:00",
          "tree_id": "ccb860d192b362baf5d11235c1ce3db34a0d2c39",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ffbb46cb8b4713afec451b9a02dc3de9585069dd"
        },
        "date": 1781652833000,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 87305.5617833523,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 53666.72534228637,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 43544.85041211717,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 124979.19096470438,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "153850349b435011c16eebed4de044bcc0ddaa7e",
          "message": "Refactored ClassifyOutcome in favor of ShouldResumeOnRecovery",
          "timestamp": "2026-06-17T16:57:01+02:00",
          "tree_id": "071b5f9af921a0da084c38210926e4a7e5b3f2b1",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/153850349b435011c16eebed4de044bcc0ddaa7e"
        },
        "date": 1781708377140,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 56801.59317108527,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 26372.47641773159,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 26895.89427718844,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 85021.08692998037,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "2f952d5b7b7f9263e434757c1b0da1d42fedca63",
          "message": "chore: cleanup/reanming",
          "timestamp": "2026-06-17T17:21:49+02:00",
          "tree_id": "2b738c83dddae1dd408f3e58fb597329bf65ea65",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/2f952d5b7b7f9263e434757c1b0da1d42fedca63"
        },
        "date": 1781709869181,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 68716.15815106258,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 40477.242884505475,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 30784.621185385236,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 91511.93828142044,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ebc07896c052d7f805ef223c02dfa22989cccdcd",
          "message": "Implemented Source-generated logging and extension",
          "timestamp": "2026-06-18T01:03:17+02:00",
          "tree_id": "c4d501288f636132f27750f885d5bea3642ddf68",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ebc07896c052d7f805ef223c02dfa22989cccdcd"
        },
        "date": 1781737524490,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 92093.25952856723,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 53688.91141436994,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 47093.59078834294,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 105412.39966057207,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c4f5bf04253150fa84ede2b59904e7c1c0f842d6",
          "message": "Optimized reflection invocation",
          "timestamp": "2026-06-18T01:43:17+02:00",
          "tree_id": "13edc6b568bd797ef2ecbbe5b0c2d47caf7159fc",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c4f5bf04253150fa84ede2b59904e7c1c0f842d6"
        },
        "date": 1781739915619,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 119423.46174221634,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 106029.47195202378,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 81712.00375482999,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 119124.25565208856,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1076187437db7250b4004ade902f9a29f3be0a3a",
          "message": "Implemented AsyncResponseDiagnostics",
          "timestamp": "2026-06-18T02:23:19+02:00",
          "tree_id": "74343527b6844a6ec95393f0eaf58e8f09e84d2e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1076187437db7250b4004ade902f9a29f3be0a3a"
        },
        "date": 1781742328279,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 115723.91791193317,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 79839.04448631559,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 106817.15637348064,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 132665.0552284625,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "d5b3851d0d74c8482c267762e70caf7f58f4f293",
          "message": "IAsyncResponsePublisher.SetResponse<T> is now constrained to where T : IAsyncResponsePayload",
          "timestamp": "2026-06-18T10:03:56+02:00",
          "tree_id": "50180d8491650ae9fa26b68bcc8b5adf95ebbeab",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/d5b3851d0d74c8482c267762e70caf7f58f4f293"
        },
        "date": 1781770138301,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 60247.96132948453,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 39782.50110993178,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 26367.683851048743,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 55130.0556604068,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e",
          "message": "fixed in-memory waiter timer could race cleanup",
          "timestamp": "2026-06-18T11:55:14+02:00",
          "tree_id": "b367b0e04256a6f5a8564598ae67835b375bc3d6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e"
        },
        "date": 1781776850889,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 98360.95243303684,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 69036.74411668867,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 80189.11801592876,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 189350.0446108705,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 63271.9494188851,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 53466.32904462086,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 19656.46784280487,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9271.767560495966,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 125440.29543698381,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 54979.8553809884,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 804712.3957897448,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b",
          "message": "Implemented the performance/allocation",
          "timestamp": "2026-06-18T14:35:14+02:00",
          "tree_id": "a1588a9b54a77bae3df547ec3acfa5e1dc0ea3a7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b"
        },
        "date": 1781786633900,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 69310.75170559897,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 58905.720216678805,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 37821.16790253636,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 103739.60529154979,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 45988.78462311639,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 43873.5347116877,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 12362.161894872175,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 8996.745877016283,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 136399.59625719508,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 60056.453065881935,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 527456.761731957,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "15bd5d6f3d11509bb5892de59d762889abbd5404",
          "message": "fix stress test harness",
          "timestamp": "2026-06-18T15:00:25+02:00",
          "tree_id": "991639655faaef8013e8004d3ff5bacaaa0ed662",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/15bd5d6f3d11509bb5892de59d762889abbd5404"
        },
        "date": 1781788122273,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 211900.68130307054,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 124052.85644107242,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 71062.61924307508,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 218721.70287968995,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 79466.49374757627,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 70765.23830335529,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 26667.064894835763,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9422.317701708265,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 197181.09900857345,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 112134.27406513656,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1194086.8817615171,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "677e678174e24eb024297dbe53c45145c2ecf137",
          "message": "Performance improvements",
          "timestamp": "2026-06-18T15:51:13+02:00",
          "tree_id": "af70395d47c9bcf88dc29a7d5f10ed4abe38030b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/677e678174e24eb024297dbe53c45145c2ecf137"
        },
        "date": 1781791005828,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 110751.04715115081,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 83556.14973262032,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 76787.50562852416,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 213002.34472981078,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 104962.01634552488,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 72493.26972483874,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 29541.03628064646,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9473.559059825053,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 194828.4730123599,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 95026.32229127469,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1082708.0694232413,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "394284cca7942c20224fd400d3e489fbe22529d4",
          "message": "Restored the typed DispatchResponseCoreAsync",
          "timestamp": "2026-06-18T19:10:42+02:00",
          "tree_id": "6a2302c9e30b9081bd4153efd804c14d2ad8f0d0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/394284cca7942c20224fd400d3e489fbe22529d4"
        },
        "date": 1781803142177,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 148455.52806818977,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 60095.72046355435,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 63765.16463145265,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 190017.10914050703,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 84530.02489578293,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 72947.1220901393,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 23950.397768206134,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9414.112696343089,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 184251.64352466023,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 88159.47696745505,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1196157.9406944893,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1bc203254299568b68f861be578ab6ac00ba7f06",
          "message": "extensive test coverage",
          "timestamp": "2026-06-18T20:04:38+02:00",
          "tree_id": "689e907f6b8413b6cd79ed03ad31498cb02d2b7a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1bc203254299568b68f861be578ab6ac00ba7f06"
        },
        "date": 1781806372737,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 144006.47913950947,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 106577.99377584517,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 80918.77114812084,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 280023.56678338046,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 137815.99139587203,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 85605.06343677621,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 36385.86481923502,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9521.134538391594,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 236811.94289990433,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 144451.0570928358,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1693480.1016088063,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b2b7198ad04d615dc67c887a8833153b5f000da4",
          "message": "Optimized context propagator and InMemoryRecoveryStateStore",
          "timestamp": "2026-06-18T20:59:24+02:00",
          "tree_id": "5dfc241eabebd6663bd8ddf74ea99542b923a9cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b2b7198ad04d615dc67c887a8833153b5f000da4"
        },
        "date": 1781809656422,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 243605.12194385196,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 185391.1753800519,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 96664.86870010884,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 329793.9842938913,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 111980.62824715827,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 86598.44095094787,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 38773.564245934824,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9544.316168835136,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 254844.5957655022,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 134948.39573347152,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1635858.0075249467,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ba59b12ce2698fbd413df7e07a67c7581f23b3fd",
          "message": "Fixed SetResponseCore/SetRawResponseJsonCore code duplication",
          "timestamp": "2026-06-19T11:33:23+02:00",
          "tree_id": "a38526b7b76cd516a7f2fd80dd5f06f0b5380b1a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ba59b12ce2698fbd413df7e07a67c7581f23b3fd"
        },
        "date": 1781862075909,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 160428.97424280734,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 113894.17864074132,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 61561.52532715026,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 186779.03293287908,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 79879.91173589272,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 81276.4961377409,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 21937.706740948328,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9302.247702112261,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 183621.0062431142,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 87050.38476270065,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1275380.0632588512,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a",
          "message": "Fixed latency regression",
          "timestamp": "2026-06-19T12:12:07+02:00",
          "tree_id": "26379ae08cbe1cd71814d960a21bc6579c351a33",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a"
        },
        "date": 1781864284691,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 106794.02209781905,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 83661.00560528738,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 63817.36593056313,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 221864.08428705306,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 107337.22953164902,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 86025.59020436928,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 32285.84283871594,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9571.251024722062,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 205183.76257776466,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 87364.32320606099,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1477978.1259237363,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a95672f4df5a479d9f48b963078061da6aa84509",
          "message": "SetResponse hot path optimization",
          "timestamp": "2026-06-19T12:57:06+02:00",
          "tree_id": "1f0bcae2122dbcfdead7271a7134b90f5ffed9dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a95672f4df5a479d9f48b963078061da6aa84509"
        },
        "date": 1781867083311,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 173536.03267475253,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 119037.415840547,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 51561.91348323413,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 187519.3144893924,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 100617.26680841627,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 64970.70600807508,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 27594.123555447633,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9481.108417422865,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 216184.43126199822,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 88841.50675195451,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1507636.177237709,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b5b9a73f55bc399e5ec9114ed56d830f24d4d855",
          "message": "InMemoryAsyncResponseChannel perf optimization",
          "timestamp": "2026-06-19T13:53:24+02:00",
          "tree_id": "430c87e41f483954b7a059e3043d430bf4efa341",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b5b9a73f55bc399e5ec9114ed56d830f24d4d855"
        },
        "date": 1781870476593,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 166726.2435110146,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 126500.29348068088,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 68064.0858761849,
            "unit": "jobs/s"
          },
          {
            "name": "race-burst throughput",
            "value": 364195.23778307077,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 135070.59329487965,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 64515.063493144895,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 28135.79007549395,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9486.959462696565,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 190248.6168925552,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 85828.36387106519,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1194957.2802772303,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "fe36405f03e470bc58ff4f78f9f2ba20725d526d",
          "message": "Implemented the opt-in Google Pub/Sub early-ACK path",
          "timestamp": "2026-06-19T15:46:06+02:00",
          "tree_id": "b43d143ce70f99303c7376e6623a0ff6882088e9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/fe36405f03e470bc58ff4f78f9f2ba20725d526d"
        },
        "date": 1781877158135,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 107047.49311898714,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 107476.49489056742,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 51372.107904441305,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 273104.65370329906,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 140275.9845831082,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 69771.43992623205,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 85285.69342727041,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 27285.832409980765,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9424.6887260931,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 194532.8488168512,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 104330.98797272371,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1256486.6121351477,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047",
          "message": "Implemented the fixes and regression coverage",
          "timestamp": "2026-06-19T16:58:25+02:00",
          "tree_id": "8f76b30848a0900974d4f94cc10a6d8d6cbcc076",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047"
        },
        "date": 1781881630120,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 107798.67347264371,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 100992.96281035137,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 50985.63387991292,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 177928.34469702363,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 142764.30526891656,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 74979.31320748606,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 100624.8400065044,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 26300.463224798685,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9441.662563477477,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 198860.92462375513,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 89393.62516180245,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1428898.0338363054,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6200531259c7c8066e4f520476472ce14710c076",
          "message": "Implement strict selection for transports, removed WithWorkerTransport",
          "timestamp": "2026-06-21T21:53:07+02:00",
          "tree_id": "c7cf5368e928b7d4cd20ddf2f339c3f7e18a4d48",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6200531259c7c8066e4f520476472ce14710c076"
        },
        "date": 1782072142919,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 70446.98329110272,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 58899.47508787801,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 30950.76797521562,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 130717.58726706127,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 72632.57042018232,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 33966.962102245176,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 38621.27605931981,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 9946.237799745792,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9078.179008072771,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 147505.6789686403,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 57381.30104066728,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 904420.8089139714,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c49198ec3adbfa666c5279ae16ddaec36180fb44",
          "message": "Implemented RabbitMQ transport (missed files)",
          "timestamp": "2026-06-21T22:44:41+02:00",
          "tree_id": "2973c02c0d02631a4fd163030b99b7e9cb1d0a07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c49198ec3adbfa666c5279ae16ddaec36180fb44"
        },
        "date": 1782075225349,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 109623.01522049792,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 98213.3035812499,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 82289.76200155035,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 346865.7213419541,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 248710.68381510253,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 90612.57361818544,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 80392.98664447392,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 25333.47979462452,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9397.886791176135,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 183772.1813022832,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 92464.51212024826,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1260890.9455421201,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "686b85a23c07f19ba921f2a1e68c232abd8cb596",
          "message": "Added missed RabbitMQ transport tests",
          "timestamp": "2026-06-21T23:33:43+02:00",
          "tree_id": "e978f3421bf474f0c4b15d8bcb963ef385f57f0a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/686b85a23c07f19ba921f2a1e68c232abd8cb596"
        },
        "date": 1782078174436,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 104358.38171040048,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 94508.3110608747,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 54043.56083943053,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 292764.0438911855,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 353566.7816937263,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 162501.5356395118,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 65454.22746600765,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 72637.42419558411,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 18676.00566555308,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9471.625378273038,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 186428.03877703205,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 120677.33776138711,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1338508.901084192,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "37bf44eb46beda36fbd0802457d5b3c60f41af02",
          "message": "Fixed RabbitMQ issues",
          "timestamp": "2026-06-22T00:04:25+02:00",
          "tree_id": "b9335f02d1174db66f8774b231c9e6b2280c2856",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/37bf44eb46beda36fbd0802457d5b3c60f41af02"
        },
        "date": 1782080017313,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 124915.7443304491,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 126769.06226389263,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 56688.725383487894,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 262351.50904588006,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 281522.0206524554,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 242074.71587690598,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 94514.99253520589,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 64973.812954426845,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 24274.078818128117,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9211.283822682786,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 182890.25121804906,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 103512.81074545786,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1638699.5280545359,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "cafc92ff97dac0cce4e301ef5228322f2c979cef",
          "message": "Implemented the recoverable-builder split",
          "timestamp": "2026-06-22T12:25:07+02:00",
          "tree_id": "0274ba7fb878a5231da1a658de8558283e00dfb7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/cafc92ff97dac0cce4e301ef5228322f2c979cef"
        },
        "date": 1782124316314,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 109934.22854974326,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 108900.19514914972,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 52927.85235076939,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 243256.91822675435,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 435532.4820125085,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 206183.01629258195,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 87690.59111525946,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 74897.30082111408,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 24573.69552994649,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9361.12213643274,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 206794.43805679402,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 87642.72617958345,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1522556.6771723079,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "4c47254f985de03b7e3448345a6c611fc96d9287",
          "message": "more test coverage",
          "timestamp": "2026-06-22T13:12:19+02:00",
          "tree_id": "4e7d907efc034ba9796d2580da02bc18c268bf07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/4c47254f985de03b7e3448345a6c611fc96d9287"
        },
        "date": 1782127288990,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 121982.2803660249,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 122443.38218007992,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 75955.935834856,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 315792.13298638305,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 536503.7126056912,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 235882.6587561058,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 95065.4202195783,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 71032.57497062092,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 25748.328830465587,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 9360.587620248449,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 197434.92544857215,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 99493.77566939413,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1538414.2026399188,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "442bd30053d373021f390964218348a9512de5b2",
          "message": "Implemented the Redis transport end to end (missed files)",
          "timestamp": "2026-06-22T18:54:51+02:00",
          "tree_id": "cf38fd5a6b520278ac71920f6f329eb3ce9501f0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/442bd30053d373021f390964218348a9512de5b2"
        },
        "date": 1782147893472,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 214547.15531926762,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 149562.6787274011,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 79447.52824439076,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 301139.51191307907,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 525165.9524409714,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 380147.192993127,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 262691.9621513421,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 116878.95781370897,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 95107.51714598319,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 30310.731065735003,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4858.57891425337,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 208225.75002915162,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 96443.17568088882,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1584710.711059696,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5a70579754ca2f51207cb9c6fd9f4681d0d529cf",
          "message": "Redis transport fixes",
          "timestamp": "2026-06-22T19:26:49+02:00",
          "tree_id": "0aff004e6924fb9cb656f2c8e583e4ae6b4a19d9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5a70579754ca2f51207cb9c6fd9f4681d0d529cf"
        },
        "date": 1782149660687,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 162503.64820690226,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 158296.97779409995,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 93394.22636892588,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 360687.90397045243,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 564499.7403301195,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 338638.67253640364,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 319989.3507544069,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 142596.4764981043,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 102816.68361085613,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 41153.56403033643,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4838.885672374051,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 241606.5871619924,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 144117.96920487235,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1987202.4164381386,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "676e73cbdca8c6975ee7b9b6d61220e8659b4642",
          "message": "package bump",
          "timestamp": "2026-06-22T19:38:14+02:00",
          "tree_id": "651f1d6e0d6e1c83abe1535756905c38d937d452",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/676e73cbdca8c6975ee7b9b6d61220e8659b4642"
        },
        "date": 1782150350125,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 122399.44029183943,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 144978.80409884074,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 73076.21030281029,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 304332.477144631,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 481435.83423201356,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 355532.80145626236,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 250848.87258482704,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 116751.61514184387,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 73905.80970613867,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 31174.526471412708,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4843.959122797754,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 208474.0533193239,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 135119.79721220833,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 2408013.8701598924,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a619da4e5271a0eb7adb52cc27fac70e8b3bf35d",
          "message": "Implemented NATS channel and transport",
          "timestamp": "2026-06-23T10:20:25+02:00",
          "tree_id": "7df45f29e08480bf8ccf543a91c7de47105bb6dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a619da4e5271a0eb7adb52cc27fac70e8b3bf35d"
        },
        "date": 1782203322201,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 140528.3867341203,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 131944.9209122144,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 91583.47834050737,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 326200.4175365344,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 415275.4937625621,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 407996.73602611176,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 238802.77660764416,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 122025.50625942036,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 88965.43870637135,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 43510.950836106436,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4869.2520609718,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 243785.89747340293,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 150553.4344249461,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 2118374.7828665846,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "78706e145b78cb241e547ddcdb73c8913a921b9b",
          "message": "Fixed NATS issues, added tests",
          "timestamp": "2026-06-23T11:24:19+02:00",
          "tree_id": "d0993e96aaccecda7fc145e3f8a483a1aadca718",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/78706e145b78cb241e547ddcdb73c8913a921b9b"
        },
        "date": 1782207142976,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 164706.76595629737,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 101878.22699283999,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 82165.2718290121,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 314137.44141336717,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 498932.284910292,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 337682.6863333063,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 442869.7962798937,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 197201.00777603014,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 79449.14412614999,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 72511.35237732819,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 28479.78550392588,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4832.741241985503,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 172255.79296231733,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 85379.59769133567,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1603232.1159457467,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "21a784c103f1c1c705195a58da20adc6425d007c",
          "message": "optimization",
          "timestamp": "2026-06-23T16:27:39+02:00",
          "tree_id": "7fbeb262c123e61793cda0685899f2f4bc34523e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/21a784c103f1c1c705195a58da20adc6425d007c"
        },
        "date": 1782225523418,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 145010.50746137064,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 135697.73059115358,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 65302.79600451373,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 267769.1615612013,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 461101.47921354533,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 316543.847653777,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 356831.90362683946,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 244321.2413864546,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 88391.53916187143,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 65523.17371189303,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 26556.441147952097,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4852.508018769501,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 238914.37308868504,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 122328.94743280472,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 2373267.5147142587,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7be0409f6dd42811feab06ca36700a5651c6f75f",
          "message": "refactor: hardening and optimizations",
          "timestamp": "2026-06-24T16:06:48+02:00",
          "tree_id": "013ba14d18b3283a0e58dc77fc4bc1f420a710d5",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7be0409f6dd42811feab06ca36700a5651c6f75f"
        },
        "date": 1782310496365,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 159079.5276737928,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 129375.47868927116,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 60464.18597429208,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 230298.00561927134,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 455132.989859637,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 287816.16605841514,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 351044.70905414515,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 198658.18316761267,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 90709.06916530241,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 71086.86701410136,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 22605.76059884649,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4829.143688997351,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 315437.51182890666,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 120165.73257837209,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 2336121.1045180582,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "67a76d645b0d5cd5117227fd338420064fcc2d6c",
          "message": "fixed loadtest apphost logging error",
          "timestamp": "2026-06-24T16:43:48+02:00",
          "tree_id": "0ffcee11ee0bd58b08b2ed5d84cf646530dd8bfe",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/67a76d645b0d5cd5117227fd338420064fcc2d6c"
        },
        "date": 1782312699987,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 112824.23428448677,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 94022.78360092218,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 53602.644454302565,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 205986.80036583255,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 260348.86748242646,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 251210.83623063163,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 297544.6614536842,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 188850.85013098698,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 85792.13594965031,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 60302.690561326395,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 21874.24861955992,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4857.73397397129,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 305213.0387010133,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 126084.96109018101,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 2181167.797238642,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd",
          "message": "Made lost-subscriber recovery coherent for multiple recoverable waiters on one correlation id",
          "timestamp": "2026-06-29T10:36:44+02:00",
          "tree_id": "666953ca1f47ae96e2377b15a22bac3f0303adcd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd"
        },
        "date": 1782722832222,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 127185.03896949594,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 91513.07721873456,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 58546.59711955426,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 208000.53248136313,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 388415.88465601887,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 320813.06864116417,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 319382.69712300063,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 209379.18234916744,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 82895.36946466171,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 54632.62835389705,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 25139.261452743616,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4855.087767849123,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 266496.1091568063,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 121711.35907772,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 968720.030224065,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5ff27f3ad385c13043a4bb6d7baabc612d9e9c53",
          "message": "Fixed exception stack traces are reset",
          "timestamp": "2026-06-29T10:52:54+02:00",
          "tree_id": "71de51782d1d8ea53d8f6f74204f34cc90f51af8",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5ff27f3ad385c13043a4bb6d7baabc612d9e9c53"
        },
        "date": 1782723656408,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 136236.73803473602,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 121460.04693216212,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 72519.07179069023,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 314663.3102580239,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 537750.0537750054,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 379523.9251882439,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 459339.28637048474,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 222745.52571062505,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 89100.13144051391,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 59032.49053827241,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 20182.403721053994,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4863.504529503356,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 246130.82345528295,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 115641.69576982678,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 893295.8149091072,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7fa908b6b83b2dde7e515f43228a49722069fd49",
          "message": "In-Memory performance fix",
          "timestamp": "2026-06-29T11:23:34+02:00",
          "tree_id": "6915c4f995598ebc2c12837e4143aedddc85a38b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7fa908b6b83b2dde7e515f43228a49722069fd49"
        },
        "date": 1782725517675,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 178848.38807524796,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 135708.77981521894,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 69616.39694489834,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 217648.6975901936,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 436026.2313380773,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 244252.73318808438,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 344343.131043222,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 238267.6985246464,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 93433.2845237855,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 57142.491430911985,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 19468.530910887395,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4819.696363948768,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 271308.5755214551,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 117217.90339369273,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1567201.6048144433,
            "unit": "entries/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d",
          "message": "upgrade libs",
          "timestamp": "2026-06-29T11:35:35+02:00",
          "tree_id": "e0664c0bccd14d40756a5d087f5bc6ac66a281e6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d"
        },
        "date": 1782726231972,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "waiter-storm throughput",
            "value": 178804.25725784362,
            "unit": "ops/s"
          },
          {
            "name": "progress-storm throughput",
            "value": 118320.23133971631,
            "unit": "ops/s"
          },
          {
            "name": "worker-storm throughput",
            "value": 83573.35601179922,
            "unit": "jobs/s"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm throughput",
            "value": 365973.26931240945,
            "unit": "ops/s"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm throughput",
            "value": 527927.3571956499,
            "unit": "ops/s"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm throughput",
            "value": 386518.2436611008,
            "unit": "ops/s"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm throughput",
            "value": 470996.0624729177,
            "unit": "ops/s"
          },
          {
            "name": "race-burst throughput",
            "value": 230837.27449506652,
            "unit": "ops/s"
          },
          {
            "name": "raw-ingress-storm throughput",
            "value": 81719.00169452523,
            "unit": "ops/s"
          },
          {
            "name": "shared-response-fanout throughput",
            "value": 62184.59971026951,
            "unit": "ops/s"
          },
          {
            "name": "exception-fanout throughput",
            "value": 22729.165446868123,
            "unit": "ops/s"
          },
          {
            "name": "timeout-storm throughput",
            "value": 4833.9642059452435,
            "unit": "ops/s"
          },
          {
            "name": "dispose-cleanup-storm throughput",
            "value": 252637.53587453012,
            "unit": "ops/s"
          },
          {
            "name": "context-isolation-storm throughput",
            "value": 113236.43314294516,
            "unit": "ops/s"
          },
          {
            "name": "watchdog-scan-storm throughput",
            "value": 1603026.5140585424,
            "unit": "entries/s"
          }
        ]
      }
    ],
    "AsyncResponse Stress - latency & allocations": [
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "336277848e28b1e7d6850920480786681c7f088c",
          "message": "added workflow yml to track performance",
          "timestamp": "2026-06-16T22:03:55+02:00",
          "tree_id": "e47c587b1108eb0a00844a032f8a52d7d22e63c2",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/336277848e28b1e7d6850920480786681c7f088c"
        },
        "date": 1781641104515,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0338,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2831.81088,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 2.08,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 5185.5968,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 4592.96352,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0321,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2691.65872,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "22035f2a553c97a781d1704f559d5e08c2930f5b",
          "message": "performance fix",
          "timestamp": "2026-06-16T23:11:02+02:00",
          "tree_id": "f890deb570c7a8d0e6511f623cf7b288fcf08580",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/22035f2a553c97a781d1704f559d5e08c2930f5b"
        },
        "date": 1781644400878,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0417,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2592.41504,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.4486,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3825.5248,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 4581.13648,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0325,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2440.73728,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ffbb46cb8b4713afec451b9a02dc3de9585069dd",
          "message": "added NBomber load test",
          "timestamp": "2026-06-17T01:31:48+02:00",
          "tree_id": "ccb860d192b362baf5d11235c1ce3db34a0d2c39",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ffbb46cb8b4713afec451b9a02dc3de9585069dd"
        },
        "date": 1781652834336,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0408,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2599.1328,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0588,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3831.7488,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 4591.94704,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0394,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2450.09856,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "153850349b435011c16eebed4de044bcc0ddaa7e",
          "message": "Refactored ClassifyOutcome in favor of ShouldResumeOnRecovery",
          "timestamp": "2026-06-17T16:57:01+02:00",
          "tree_id": "071b5f9af921a0da084c38210926e4a7e5b3f2b1",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/153850349b435011c16eebed4de044bcc0ddaa7e"
        },
        "date": 1781708380897,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0425,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2523.58416,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 2.0584,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3879.0576,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 4600.80448,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0294,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2387.73632,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "2f952d5b7b7f9263e434757c1b0da1d42fedca63",
          "message": "chore: cleanup/reanming",
          "timestamp": "2026-06-17T17:21:49+02:00",
          "tree_id": "2b738c83dddae1dd408f3e58fb597329bf65ea65",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/2f952d5b7b7f9263e434757c1b0da1d42fedca63"
        },
        "date": 1781709870964,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0318,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2528.92896,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 2.0428,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3768.6464,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 4599.16032,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0224,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2388.21408,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ebc07896c052d7f805ef223c02dfa22989cccdcd",
          "message": "Implemented Source-generated logging and extension",
          "timestamp": "2026-06-18T01:03:17+02:00",
          "tree_id": "c4d501288f636132f27750f885d5bea3642ddf68",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ebc07896c052d7f805ef223c02dfa22989cccdcd"
        },
        "date": 1781737526585,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.04,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2590.00976,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0722,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3823.8128,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 4473.25856,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0388,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2455.50992,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c4f5bf04253150fa84ede2b59904e7c1c0f842d6",
          "message": "Optimized reflection invocation",
          "timestamp": "2026-06-18T01:43:17+02:00",
          "tree_id": "13edc6b568bd797ef2ecbbe5b0c2d47caf7159fc",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c4f5bf04253150fa84ede2b59904e7c1c0f842d6"
        },
        "date": 1781739917557,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0402,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2592.512,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0606,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3817.2576,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 3094.90544,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0355,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2447.10192,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1076187437db7250b4004ade902f9a29f3be0a3a",
          "message": "Implemented AsyncResponseDiagnostics",
          "timestamp": "2026-06-18T02:23:19+02:00",
          "tree_id": "74343527b6844a6ec95393f0eaf58e8f09e84d2e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1076187437db7250b4004ade902f9a29f3be0a3a"
        },
        "date": 1781742329577,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0305,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2603.08128,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0511,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3846.8624,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 3106.09792,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0284,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2458.98192,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "d5b3851d0d74c8482c267762e70caf7f58f4f293",
          "message": "IAsyncResponsePublisher.SetResponse<T> is now constrained to where T : IAsyncResponsePayload",
          "timestamp": "2026-06-18T10:03:56+02:00",
          "tree_id": "50180d8491650ae9fa26b68bcc8b5adf95ebbeab",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/d5b3851d0d74c8482c267762e70caf7f58f4f293"
        },
        "date": 1781770141847,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0427,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2536.34224,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 2.0528,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3760.0128,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 3120.13488,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0396,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2401.75552,
            "unit": "B/op"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e",
          "message": "fixed in-memory waiter timer could race cleanup",
          "timestamp": "2026-06-18T11:55:14+02:00",
          "tree_id": "b367b0e04256a6f5a8564598ae67835b375bc3d6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e"
        },
        "date": 1781776853080,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0327,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 2639.83552,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0457,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 3876.9984,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 3103.91232,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0225,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 2492.05376,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0628,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 3072.28,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 2.2655,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 5760.3328,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 4.8055,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 9735.8336,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.7403,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 3691.532,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.026,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 2011.8496,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0567,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 3788.592,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 12.4268,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 93.5,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b",
          "message": "Implemented the performance/allocation",
          "timestamp": "2026-06-18T14:35:14+02:00",
          "tree_id": "a1588a9b54a77bae3df547ec3acfa5e1dc0ea3a7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b"
        },
        "date": 1781786637147,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.025,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1986.08384,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0329,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2776.6144,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 3113.70496,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.018,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1842.424,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.1704,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 2433.32672,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 2.0383,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4990.14016,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 6.0721,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8954.28864,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 30.0687,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 3160.984,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0128,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1434.496,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0475,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 3113.024,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 18.9589,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 90.8048,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "15bd5d6f3d11509bb5892de59d762889abbd5404",
          "message": "fix stress test harness",
          "timestamp": "2026-06-18T15:00:25+02:00",
          "tree_id": "991639655faaef8013e8004d3ff5bacaaa0ed662",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/15bd5d6f3d11509bb5892de59d762889abbd5404"
        },
        "date": 1781788124365,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0316,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1673.69056,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0367,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2500.128,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.89248,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.018,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.59904,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0609,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 2114.55808,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.4225,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4635.01184,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1385,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8502.78016,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.4091,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2748.292,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0296,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1144.0608,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 2.1173,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2741.168,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 8.3746,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 72.3784,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "677e678174e24eb024297dbe53c45145c2ecf137",
          "message": "Performance improvements",
          "timestamp": "2026-06-18T15:51:13+02:00",
          "tree_id": "af70395d47c9bcf88dc29a7d5f10ed4abe38030b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/677e678174e24eb024297dbe53c45145c2ecf137"
        },
        "date": 1781791008237,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0295,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1674.07104,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0367,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2498.1472,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.26272,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0233,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.04352,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0356,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1912.92288,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0426,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4637.25184,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0912,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8504.64128,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 27.859,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2740.46,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0224,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1146.384,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0409,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2727.6384,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 9.2361,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 72.3784,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "394284cca7942c20224fd400d3e489fbe22529d4",
          "message": "Restored the typed DispatchResponseCoreAsync",
          "timestamp": "2026-06-18T19:10:42+02:00",
          "tree_id": "6a2302c9e30b9081bd4153efd804c14d2ad8f0d0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/394284cca7942c20224fd400d3e489fbe22529d4"
        },
        "date": 1781803143564,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0367,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1674.12288,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0441,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2502.4512,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.94656,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0301,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.04512,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0526,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1915.41504,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.1022,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4636.06144,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 4.3364,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8500.99456,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 29.6587,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2743.82,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0311,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1130.9888,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0529,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2704.3904,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 8.3601,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 71.56,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1bc203254299568b68f861be578ab6ac00ba7f06",
          "message": "extensive test coverage",
          "timestamp": "2026-06-18T20:04:38+02:00",
          "tree_id": "689e907f6b8413b6cd79ed03ad31498cb02d2b7a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1bc203254299568b68f861be578ab6ac00ba7f06"
        },
        "date": 1781806374411,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0218,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1674.09344,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0253,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2495.936,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.80032,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0114,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.00896,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.028,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1914.19552,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0432,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4635.34976,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1055,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8502.35008,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 30.0153,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2744.188,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0231,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1140.8256,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 2.3771,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2746.1536,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 5.905,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 72.3784,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b2b7198ad04d615dc67c887a8833153b5f000da4",
          "message": "Optimized context propagator and InMemoryRecoveryStateStore",
          "timestamp": "2026-06-18T20:59:24+02:00",
          "tree_id": "5dfc241eabebd6663bd8ddf74ea99542b923a9cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b2b7198ad04d615dc67c887a8833153b5f000da4"
        },
        "date": 1781809658573,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0175,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1674.04544,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0195,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2497.0336,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.90432,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0093,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.2832,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0239,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1915.01952,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0306,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4637.05216,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0675,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8500.05248,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.4138,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2753.452,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0107,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1140.848,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 3.078,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2757.9584,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.113,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ba59b12ce2698fbd413df7e07a67c7581f23b3fd",
          "message": "Fixed SetResponseCore/SetRawResponseJsonCore code duplication",
          "timestamp": "2026-06-19T11:33:23+02:00",
          "tree_id": "a38526b7b76cd516a7f2fd80dd5f06f0b5380b1a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ba59b12ce2698fbd413df7e07a67c7581f23b3fd"
        },
        "date": 1781862078082,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0363,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1673.75712,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0441,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2496.8192,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.73888,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0358,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.29536,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0584,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1914.36032,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0797,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4635.328,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 5.1255,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8499.51232,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.5961,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2756.056,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0358,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1153.84,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0541,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2707.3888,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 7.8408,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a",
          "message": "Fixed latency regression",
          "timestamp": "2026-06-19T12:12:07+02:00",
          "tree_id": "26379ae08cbe1cd71814d960a21bc6579c351a33",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a"
        },
        "date": 1781864287484,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0359,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1673.88832,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0477,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2494.5952,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.73888,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0259,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.41536,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0515,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1916.92224,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0644,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4684.1408,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1064,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8524.44032,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 27.5941,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2761.628,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0335,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1152.9984,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0529,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2725.4368,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.766,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a95672f4df5a479d9f48b963078061da6aa84509",
          "message": "SetResponse hot path optimization",
          "timestamp": "2026-06-19T12:57:06+02:00",
          "tree_id": "1f0bcae2122dbcfdead7271a7134b90f5ffed9dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a95672f4df5a479d9f48b963078061da6aa84509"
        },
        "date": 1781867084708,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.033,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1674.14016,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0427,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2496.848,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.888,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0343,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1539.12608,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0493,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1914.89184,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 2.2402,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4684.28416,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1176,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8526.97216,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 27.873,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2762.856,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0145,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1152.7296,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0551,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2724.0736,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.6329,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b5b9a73f55bc399e5ec9114ed56d830f24d4d855",
          "message": "InMemoryAsyncResponseChannel perf optimization",
          "timestamp": "2026-06-19T13:53:24+02:00",
          "tree_id": "430c87e41f483954b7a059e3043d430bf4efa341",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b5b9a73f55bc399e5ec9114ed56d830f24d4d855"
        },
        "date": 1781870478188,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0251,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1610.10336,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0288,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2496.88,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2863.31904,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0136,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1474.99904,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.029,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1851.95488,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.9547,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4379.4624,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0957,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8502.77632,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.0592,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2742.156,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0217,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1155.984,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0505,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2725.6672,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 8.3685,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "fe36405f03e470bc58ff4f78f9f2ba20725d526d",
          "message": "Implemented the opt-in Google Pub/Sub early-ACK path",
          "timestamp": "2026-06-19T15:46:06+02:00",
          "tree_id": "b43d143ce70f99303c7376e6623a0ff6882088e9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/fe36405f03e470bc58ff4f78f9f2ba20725d526d"
        },
        "date": 1781877160923,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0316,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1609.63584,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0362,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2498.5824,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.5824,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0027,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 454.5568,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0327,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1474.86464,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0509,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1849.52384,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0594,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4378.0544,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1155,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8499.54304,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.5862,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2747.036,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0145,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1127.6256,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 3.0773,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2746.5312,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 7.9587,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047",
          "message": "Implemented the fixes and regression coverage",
          "timestamp": "2026-06-19T16:58:25+02:00",
          "tree_id": "8f76b30848a0900974d4f94cc10a6d8d6cbcc076",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047"
        },
        "date": 1781881632902,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0293,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1609.6992,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0383,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2501.5776,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.59712,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0023,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 462.4768,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0158,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1474.35072,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0502,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1850.40096,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0716,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4381.0176,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1187,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8508.30336,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.7263,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2758.288,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0265,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1156.1344,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0522,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2723.136,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.9984,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6200531259c7c8066e4f520476472ce14710c076",
          "message": "Implement strict selection for transports, removed WithWorkerTransport",
          "timestamp": "2026-06-21T21:53:07+02:00",
          "tree_id": "c7cf5368e928b7d4cd20ddf2f339c3f7e18a4d48",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6200531259c7c8066e4f520476472ce14710c076"
        },
        "date": 1782072146127,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0352,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1609.47168,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0432,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2497.0944,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.152,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0021,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 478.8672,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0326,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1474.16672,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.92,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1870.91712,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 4.0615,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4428.81536,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 6.1029,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8527.87328,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 31.9995,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2764.48,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0292,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1177.0048,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 10.0464,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2753.232,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 11.0568,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c49198ec3adbfa666c5279ae16ddaec36180fb44",
          "message": "Implemented RabbitMQ transport (missed files)",
          "timestamp": "2026-06-21T22:44:41+02:00",
          "tree_id": "2973c02c0d02631a4fd163030b99b7e9cb1d0a07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c49198ec3adbfa666c5279ae16ddaec36180fb44"
        },
        "date": 1782075226832,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0338,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1609.73344,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0394,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2494.6976,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.96352,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0016,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 461.952,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0117,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1475.49216,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0411,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1850.2928,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 1.0562,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4379.27168,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.128,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8499.31264,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.5385,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2736.208,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0222,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1124.8288,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0477,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2702.8736,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 7.9309,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "686b85a23c07f19ba921f2a1e68c232abd8cb596",
          "message": "Added missed RabbitMQ transport tests",
          "timestamp": "2026-06-21T23:33:43+02:00",
          "tree_id": "e978f3421bf474f0c4b15d8bcb963ef385f57f0a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/686b85a23c07f19ba921f2a1e68c232abd8cb596"
        },
        "date": 1782078176315,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.03,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1609.80928,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.036,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2498.128,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.4512,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 455.6128,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.002,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 348.272,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0287,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1475.51744,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0486,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1848,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0513,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4379.55328,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 6.1108,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8498.92864,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.7759,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2745.156,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0243,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1134.8992,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 2.7159,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2726.7456,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 7.471,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.5648,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "37bf44eb46beda36fbd0802457d5b3c60f41af02",
          "message": "Fixed RabbitMQ issues",
          "timestamp": "2026-06-22T00:04:25+02:00",
          "tree_id": "b9335f02d1174db66f8774b231c9e6b2280c2856",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/37bf44eb46beda36fbd0802457d5b3c60f41af02"
        },
        "date": 1782080018724,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0312,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1609.81376,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0377,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2500.4576,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.40768,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0021,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 465.7984,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.002,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 354.2112,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.015,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1475.79008,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0469,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1849.37312,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 1.0616,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4379.3792,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1125,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8500.06784,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 29.4162,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2806.256,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0361,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1160.5824,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 6.3121,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2729.0944,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.1024,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "cafc92ff97dac0cce4e301ef5228322f2c979cef",
          "message": "Implemented the recoverable-builder split",
          "timestamp": "2026-06-22T12:25:07+02:00",
          "tree_id": "0274ba7fb878a5231da1a658de8558283e00dfb7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/cafc92ff97dac0cce4e301ef5228322f2c979cef"
        },
        "date": 1782124317846,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0331,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.28448,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0431,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2485.5488,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.05536,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0029,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 466.4736,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 350.9856,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0325,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1459.10048,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0505,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1835.33952,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0674,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4380.86016,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1245,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8504.3712,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.8361,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2766.936,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0305,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1152.6272,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0462,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2711.936,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.5679,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "4c47254f985de03b7e3448345a6c611fc96d9287",
          "message": "more test coverage",
          "timestamp": "2026-06-22T13:12:19+02:00",
          "tree_id": "4e7d907efc034ba9796d2580da02bc18c268bf07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/4c47254f985de03b7e3448345a6c611fc96d9287"
        },
        "date": 1782127290843,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0312,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1594.17248,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0386,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2482.5632,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.584,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0018,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 454.1408,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0019,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 342.4288,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0132,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1459.48928,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0435,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1834.60256,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 1.0742,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4378.65472,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.2691,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8499.0272,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 28.8352,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2757.656,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0208,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1131.4592,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 2.864,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2735.9328,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.5002,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "442bd30053d373021f390964218348a9512de5b2",
          "message": "Implemented the Redis transport end to end (missed files)",
          "timestamp": "2026-06-22T18:54:51+02:00",
          "tree_id": "cf38fd5a6b520278ac71920f6f329eb3ce9501f0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/442bd30053d373021f390964218348a9512de5b2"
        },
        "date": 1782147895757,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0251,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.60192,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0243,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2484.4288,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.57728,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0021,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 456.2912,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0022,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 348.1344,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 478.848,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0156,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1458.70432,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0332,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1834.0944,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0466,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4379.45088,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0946,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8500.22784,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 52.707,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2759.144,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0102,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1186.608,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0384,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2709.6768,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.3103,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5a70579754ca2f51207cb9c6fd9f4681d0d529cf",
          "message": "Redis transport fixes",
          "timestamp": "2026-06-22T19:26:49+02:00",
          "tree_id": "0aff004e6924fb9cb656f2c8e583e4ae6b4a19d9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5a70579754ca2f51207cb9c6fd9f4681d0d529cf"
        },
        "date": 1782149662644,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0211,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.55552,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0254,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2483.408,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.53088,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0014,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 439.52,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0014,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 331.2608,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0017,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 522.4768,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0083,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1459.13504,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0243,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1833.42656,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0367,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4378.23744,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0707,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8500.52352,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 54.8346,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2762.116,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0228,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1206.1376,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 2.9625,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2753.9776,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 5.0322,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "676e73cbdca8c6975ee7b9b6d61220e8659b4642",
          "message": "package bump",
          "timestamp": "2026-06-22T19:38:14+02:00",
          "tree_id": "651f1d6e0d6e1c83abe1535756905c38d937d452",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/676e73cbdca8c6975ee7b9b6d61220e8659b4642"
        },
        "date": 1782150351617,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0269,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.29856,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0312,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2481.8304,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.00608,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0023,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 458.8224,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0022,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 339.9424,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.003,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 483.008,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0154,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1458.68416,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.033,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1837.03808,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 1.74,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4429.1968,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0966,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8522.64704,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.1588,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2774.916,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0108,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1165.6064,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.5251,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2754.32,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 4.1528,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a619da4e5271a0eb7adb52cc27fac70e8b3bf35d",
          "message": "Implemented NATS channel and transport",
          "timestamp": "2026-06-23T10:20:25+02:00",
          "tree_id": "7df45f29e08480bf8ccf543a91c7de47105bb6dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a619da4e5271a0eb7adb52cc27fac70e8b3bf35d"
        },
        "date": 1782203324241,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0208,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.49984,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0267,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2485.2768,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2808.63968,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0019,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 461.2864,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0015,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 344.384,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0018,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 492,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0096,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1459.48544,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0235,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1832.76416,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0423,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4382.07872,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.0768,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8502.2528,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.3129,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2751.192,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0092,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1156.3104,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0344,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2738.6144,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 4.7206,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "78706e145b78cb241e547ddcdb73c8913a921b9b",
          "message": "Fixed NATS issues, added tests",
          "timestamp": "2026-06-23T11:24:19+02:00",
          "tree_id": "d0993e96aaccecda7fc145e3f8a483a1aadca718",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/78706e145b78cb241e547ddcdb73c8913a921b9b"
        },
        "date": 1782207145776,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0336,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.86144,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0402,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2478.0704,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.09824,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0017,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 460.5696,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0017,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 355.2992,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0022,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 488.5472,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0019,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 435.0944,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0284,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1458.85568,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0482,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1833.32096,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.1136,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4379.70432,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1088,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8500.06528,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 55.5595,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2745.012,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0147,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1154.3104,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.1093,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2712.6048,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.2374,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 62.7376,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "21a784c103f1c1c705195a58da20adc6425d007c",
          "message": "optimization",
          "timestamp": "2026-06-23T16:27:39+02:00",
          "tree_id": "7fbeb262c123e61793cda0685899f2f4bc34523e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/21a784c103f1c1c705195a58da20adc6425d007c"
        },
        "date": 1782225524955,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0356,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1593.32288,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0389,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2479.104,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2809.6784,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0025,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 489.6256,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0029,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 367.7248,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0032,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 564.2976,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.003,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 489.8688,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0277,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1459.56576,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0491,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1857.66528,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0641,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4427.47776,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1296,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8524.4032,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 52.632,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2769.864,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0335,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1181.9776,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 3.6096,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2755.9232,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 4.2136,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7be0409f6dd42811feab06ca36700a5651c6f75f",
          "message": "refactor: hardening and optimizations",
          "timestamp": "2026-06-24T16:06:48+02:00",
          "tree_id": "013ba14d18b3283a0e58dc77fc4bc1f420a710d5",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7be0409f6dd42811feab06ca36700a5651c6f75f"
        },
        "date": 1782310497987,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0292,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1602.02464,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0365,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2492.0512,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2816.7392,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.004,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 459.4464,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 340.5952,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0032,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 488.24,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0037,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 435.8048,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0342,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1466.41152,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0516,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1843.67552,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0691,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4414.54464,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.7794,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8534.61248,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 54.0371,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2754.208,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0248,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1161.3632,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0453,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2722.9024,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 4.2806,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "67a76d645b0d5cd5117227fd338420064fcc2d6c",
          "message": "fixed loadtest apphost logging error",
          "timestamp": "2026-06-24T16:43:48+02:00",
          "tree_id": "0ffcee11ee0bd58b08b2ed5d84cf646530dd8bfe",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/67a76d645b0d5cd5117227fd338420064fcc2d6c"
        },
        "date": 1782312702359,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0331,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1602.51904,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0407,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2489.8656,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2817.21152,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0025,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 465.5104,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0032,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 355.6896,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0038,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 492.9824,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0029,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 471.1648,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0355,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1466.75008,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0529,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1842.55104,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.316,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 4412.38016,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1304,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 8533.81248,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.8575,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2801.04,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0116,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1161.1936,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0448,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2720.7168,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 4.5847,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.556,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd",
          "message": "Made lost-subscriber recovery coherent for multiple recoverable waiters on one correlation id",
          "timestamp": "2026-06-29T10:36:44+02:00",
          "tree_id": "666953ca1f47ae96e2377b15a22bac3f0303adcd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd"
        },
        "date": 1782722834113,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0361,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1761.8768,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0426,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2651.5584,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2892.66304,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0027,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 463.76,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 353.776,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0029,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 485.6448,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 467.8432,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0327,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1626.9632,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0531,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 2007.2928,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.1433,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 5433.82912,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1272,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 9433.51168,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.7552,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 3039.452,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0269,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1417.6128,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0505,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2880.5344,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 10.3229,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 98.2472,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5ff27f3ad385c13043a4bb6d7baabc612d9e9c53",
          "message": "Fixed exception stack traces are reset",
          "timestamp": "2026-06-29T10:52:54+02:00",
          "tree_id": "71de51782d1d8ea53d8f6f74204f34cc90f51af8",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5ff27f3ad385c13043a4bb6d7baabc612d9e9c53"
        },
        "date": 1782723657798,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0392,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1762.44096,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0416,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2645.4464,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2816.95424,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0016,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 461.2672,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0018,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 346.1472,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0019,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 492.544,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0018,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 440.016,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0148,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1627.05216,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0529,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 2002.50528,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0982,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 5390.73024,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 6.0317,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 9417.00096,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.332,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 3046.048,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0293,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1420.9024,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0654,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2877.0432,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 11.1945,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 98.2472,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7fa908b6b83b2dde7e515f43228a49722069fd49",
          "message": "In-Memory performance fix",
          "timestamp": "2026-06-29T11:23:34+02:00",
          "tree_id": "6915c4f995598ebc2c12837e4143aedddc85a38b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7fa908b6b83b2dde7e515f43228a49722069fd49"
        },
        "date": 1782725519564,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0324,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1626.09856,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0435,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2513.4624,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2818.15488,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0022,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 456.9216,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0023,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 334.2688,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0027,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 523.952,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0026,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 451.8432,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0307,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1491.44896,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.051,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1868.15424,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 2.0754,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 5052.39936,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.954,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 9172.11264,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.7986,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2813.312,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0224,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1218.96,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.053,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2741.0368,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.3808,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.5576,
            "unit": "B/entry"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d",
          "message": "upgrade libs",
          "timestamp": "2026-06-29T11:35:35+02:00",
          "tree_id": "e0664c0bccd14d40756a5d087f5bc6ac66a281e6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d"
        },
        "date": 1782726234001,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "waiter-storm p99 latency",
            "value": 0.0334,
            "unit": "ms"
          },
          {
            "name": "waiter-storm allocations",
            "value": 1626.2896,
            "unit": "B/op"
          },
          {
            "name": "progress-storm p99 latency",
            "value": 0.0424,
            "unit": "ms"
          },
          {
            "name": "progress-storm allocations",
            "value": 2510.6048,
            "unit": "B/op"
          },
          {
            "name": "worker-storm allocations",
            "value": 2816.696,
            "unit": "B/op"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0016,
            "unit": "ms"
          },
          {
            "name": "google-pubsub-ack-after-enqueue-dispatch-storm allocations",
            "value": 464.768,
            "unit": "B/op"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.0016,
            "unit": "ms"
          },
          {
            "name": "rabbitmq-ack-after-enqueue-dispatch-storm allocations",
            "value": 347.0912,
            "unit": "B/op"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm p99 latency",
            "value": 0.002,
            "unit": "ms"
          },
          {
            "name": "redis-ack-after-enqueue-dispatch-storm allocations",
            "value": 483.8048,
            "unit": "B/op"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm p99 latency",
            "value": 0.0017,
            "unit": "ms"
          },
          {
            "name": "nats-ack-after-receive-dispatch-storm allocations",
            "value": 440.0448,
            "unit": "B/op"
          },
          {
            "name": "race-burst p99 latency",
            "value": 0.0266,
            "unit": "ms"
          },
          {
            "name": "race-burst allocations",
            "value": 1491.18912,
            "unit": "B/op"
          },
          {
            "name": "raw-ingress-storm p99 latency",
            "value": 0.0569,
            "unit": "ms"
          },
          {
            "name": "raw-ingress-storm allocations",
            "value": 1866.71488,
            "unit": "B/op"
          },
          {
            "name": "shared-response-fanout p99 latency",
            "value": 0.0634,
            "unit": "ms"
          },
          {
            "name": "shared-response-fanout allocations",
            "value": 5051.84256,
            "unit": "B/op"
          },
          {
            "name": "exception-fanout p99 latency",
            "value": 3.1178,
            "unit": "ms"
          },
          {
            "name": "exception-fanout allocations",
            "value": 9171.7248,
            "unit": "B/op"
          },
          {
            "name": "timeout-storm p99 latency",
            "value": 53.7755,
            "unit": "ms"
          },
          {
            "name": "timeout-storm allocations",
            "value": 2773.856,
            "unit": "B/op"
          },
          {
            "name": "dispose-cleanup-storm p99 latency",
            "value": 0.0264,
            "unit": "ms"
          },
          {
            "name": "dispose-cleanup-storm allocations",
            "value": 1186.4704,
            "unit": "B/op"
          },
          {
            "name": "context-isolation-storm p99 latency",
            "value": 0.0484,
            "unit": "ms"
          },
          {
            "name": "context-isolation-storm allocations",
            "value": 2744.8192,
            "unit": "B/op"
          },
          {
            "name": "watchdog-scan-storm elapsed",
            "value": 6.2382,
            "unit": "ms"
          },
          {
            "name": "watchdog-scan-storm allocations",
            "value": 63.5576,
            "unit": "B/entry"
          }
        ]
      }
    ],
    "AsyncResponse Load test - throughput": [
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ffbb46cb8b4713afec451b9a02dc3de9585069dd",
          "message": "added NBomber load test",
          "timestamp": "2026-06-17T01:31:48+02:00",
          "tree_id": "ccb860d192b362baf5d11235c1ce3db34a0d2c39",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ffbb46cb8b4713afec451b9a02dc3de9585069dd"
        },
        "date": 1781652976425,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "153850349b435011c16eebed4de044bcc0ddaa7e",
          "message": "Refactored ClassifyOutcome in favor of ShouldResumeOnRecovery",
          "timestamp": "2026-06-17T16:57:01+02:00",
          "tree_id": "071b5f9af921a0da084c38210926e4a7e5b3f2b1",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/153850349b435011c16eebed4de044bcc0ddaa7e"
        },
        "date": 1781708557004,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "2f952d5b7b7f9263e434757c1b0da1d42fedca63",
          "message": "chore: cleanup/reanming",
          "timestamp": "2026-06-17T17:21:49+02:00",
          "tree_id": "2b738c83dddae1dd408f3e58fb597329bf65ea65",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/2f952d5b7b7f9263e434757c1b0da1d42fedca63"
        },
        "date": 1781710040711,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ebc07896c052d7f805ef223c02dfa22989cccdcd",
          "message": "Implemented Source-generated logging and extension",
          "timestamp": "2026-06-18T01:03:17+02:00",
          "tree_id": "c4d501288f636132f27750f885d5bea3642ddf68",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ebc07896c052d7f805ef223c02dfa22989cccdcd"
        },
        "date": 1781737668812,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c4f5bf04253150fa84ede2b59904e7c1c0f842d6",
          "message": "Optimized reflection invocation",
          "timestamp": "2026-06-18T01:43:17+02:00",
          "tree_id": "13edc6b568bd797ef2ecbbe5b0c2d47caf7159fc",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c4f5bf04253150fa84ede2b59904e7c1c0f842d6"
        },
        "date": 1781740066111,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1076187437db7250b4004ade902f9a29f3be0a3a",
          "message": "Implemented AsyncResponseDiagnostics",
          "timestamp": "2026-06-18T02:23:19+02:00",
          "tree_id": "74343527b6844a6ec95393f0eaf58e8f09e84d2e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1076187437db7250b4004ade902f9a29f3be0a3a"
        },
        "date": 1781742481212,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "bfc3f5edbb30e7bab9c88d15df588817189ae9f5",
          "message": "Tests coverage expansion",
          "timestamp": "2026-06-18T09:14:16+02:00",
          "tree_id": "a8647fcaa34acd2d751d8394d5fd03c12fc4fb77",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/bfc3f5edbb30e7bab9c88d15df588817189ae9f5"
        },
        "date": 1781767008881,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "d5b3851d0d74c8482c267762e70caf7f58f4f293",
          "message": "IAsyncResponsePublisher.SetResponse<T> is now constrained to where T : IAsyncResponsePayload",
          "timestamp": "2026-06-18T10:03:56+02:00",
          "tree_id": "50180d8491650ae9fa26b68bcc8b5adf95ebbeab",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/d5b3851d0d74c8482c267762e70caf7f58f4f293"
        },
        "date": 1781769986923,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_redis throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub throughput",
            "value": 50,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 50,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "972a7e1543b09e0ba9a5455b1560145ed80292c1",
          "message": "Implemented the benchmark/load-test expansion end to end",
          "timestamp": "2026-06-18T11:05:58+02:00",
          "tree_id": "485ea28888c96763ab19ee064ec892b43578e8cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/972a7e1543b09e0ba9a5455b1560145ed80292c1"
        },
        "date": 1781774044513,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e",
          "message": "fixed in-memory waiter timer could race cleanup",
          "timestamp": "2026-06-18T11:55:14+02:00",
          "tree_id": "b367b0e04256a6f5a8564598ae67835b375bc3d6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e"
        },
        "date": 1781776989843,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b",
          "message": "Implemented the performance/allocation",
          "timestamp": "2026-06-18T14:35:14+02:00",
          "tree_id": "a1588a9b54a77bae3df547ec3acfa5e1dc0ea3a7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b"
        },
        "date": 1781786265903,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "15bd5d6f3d11509bb5892de59d762889abbd5404",
          "message": "fix stress test harness",
          "timestamp": "2026-06-18T15:00:25+02:00",
          "tree_id": "991639655faaef8013e8004d3ff5bacaaa0ed662",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/15bd5d6f3d11509bb5892de59d762889abbd5404"
        },
        "date": 1781787777072,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "677e678174e24eb024297dbe53c45145c2ecf137",
          "message": "Performance improvements",
          "timestamp": "2026-06-18T15:51:13+02:00",
          "tree_id": "af70395d47c9bcf88dc29a7d5f10ed4abe38030b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/677e678174e24eb024297dbe53c45145c2ecf137"
        },
        "date": 1781791149788,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "394284cca7942c20224fd400d3e489fbe22529d4",
          "message": "Restored the typed DispatchResponseCoreAsync",
          "timestamp": "2026-06-18T19:10:42+02:00",
          "tree_id": "6a2302c9e30b9081bd4153efd804c14d2ad8f0d0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/394284cca7942c20224fd400d3e489fbe22529d4"
        },
        "date": 1781802809668,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1bc203254299568b68f861be578ab6ac00ba7f06",
          "message": "extensive test coverage",
          "timestamp": "2026-06-18T20:04:38+02:00",
          "tree_id": "689e907f6b8413b6cd79ed03ad31498cb02d2b7a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1bc203254299568b68f861be578ab6ac00ba7f06"
        },
        "date": 1781806019838,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b2b7198ad04d615dc67c887a8833153b5f000da4",
          "message": "Optimized context propagator and InMemoryRecoveryStateStore",
          "timestamp": "2026-06-18T20:59:24+02:00",
          "tree_id": "5dfc241eabebd6663bd8ddf74ea99542b923a9cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b2b7198ad04d615dc67c887a8833153b5f000da4"
        },
        "date": 1781809313032,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ba59b12ce2698fbd413df7e07a67c7581f23b3fd",
          "message": "Fixed SetResponseCore/SetRawResponseJsonCore code duplication",
          "timestamp": "2026-06-19T11:33:23+02:00",
          "tree_id": "a38526b7b76cd516a7f2fd80dd5f06f0b5380b1a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ba59b12ce2698fbd413df7e07a67c7581f23b3fd"
        },
        "date": 1781861730617,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a",
          "message": "Fixed latency regression",
          "timestamp": "2026-06-19T12:12:07+02:00",
          "tree_id": "26379ae08cbe1cd71814d960a21bc6579c351a33",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a"
        },
        "date": 1781864418426,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a95672f4df5a479d9f48b963078061da6aa84509",
          "message": "SetResponse hot path optimization",
          "timestamp": "2026-06-19T12:57:06+02:00",
          "tree_id": "1f0bcae2122dbcfdead7271a7134b90f5ffed9dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a95672f4df5a479d9f48b963078061da6aa84509"
        },
        "date": 1781866753629,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b5b9a73f55bc399e5ec9114ed56d830f24d4d855",
          "message": "InMemoryAsyncResponseChannel perf optimization",
          "timestamp": "2026-06-19T13:53:24+02:00",
          "tree_id": "430c87e41f483954b7a059e3043d430bf4efa341",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b5b9a73f55bc399e5ec9114ed56d830f24d4d855"
        },
        "date": 1781870137451,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "fe36405f03e470bc58ff4f78f9f2ba20725d526d",
          "message": "Implemented the opt-in Google Pub/Sub early-ACK path",
          "timestamp": "2026-06-19T15:46:06+02:00",
          "tree_id": "b43d143ce70f99303c7376e6623a0ff6882088e9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/fe36405f03e470bc58ff4f78f9f2ba20725d526d"
        },
        "date": 1781877293121,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047",
          "message": "Implemented the fixes and regression coverage",
          "timestamp": "2026-06-19T16:58:25+02:00",
          "tree_id": "8f76b30848a0900974d4f94cc10a6d8d6cbcc076",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047"
        },
        "date": 1781881238702,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6200531259c7c8066e4f520476472ce14710c076",
          "message": "Implement strict selection for transports, removed WithWorkerTransport",
          "timestamp": "2026-06-21T21:53:07+02:00",
          "tree_id": "c7cf5368e928b7d4cd20ddf2f339c3f7e18a4d48",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6200531259c7c8066e4f520476472ce14710c076"
        },
        "date": 1782071742511,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c49198ec3adbfa666c5279ae16ddaec36180fb44",
          "message": "Implemented RabbitMQ transport (missed files)",
          "timestamp": "2026-06-21T22:44:41+02:00",
          "tree_id": "2973c02c0d02631a4fd163030b99b7e9cb1d0a07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c49198ec3adbfa666c5279ae16ddaec36180fb44"
        },
        "date": 1782074818566,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "686b85a23c07f19ba921f2a1e68c232abd8cb596",
          "message": "Added missed RabbitMQ transport tests",
          "timestamp": "2026-06-21T23:33:43+02:00",
          "tree_id": "e978f3421bf474f0c4b15d8bcb963ef385f57f0a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/686b85a23c07f19ba921f2a1e68c232abd8cb596"
        },
        "date": 1782078313414,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "37bf44eb46beda36fbd0802457d5b3c60f41af02",
          "message": "Fixed RabbitMQ issues",
          "timestamp": "2026-06-22T00:04:25+02:00",
          "tree_id": "b9335f02d1174db66f8774b231c9e6b2280c2856",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/37bf44eb46beda36fbd0802457d5b3c60f41af02"
        },
        "date": 1782079609012,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "cafc92ff97dac0cce4e301ef5228322f2c979cef",
          "message": "Implemented the recoverable-builder split",
          "timestamp": "2026-06-22T12:25:07+02:00",
          "tree_id": "0274ba7fb878a5231da1a658de8558283e00dfb7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/cafc92ff97dac0cce4e301ef5228322f2c979cef"
        },
        "date": 1782124455113,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "4c47254f985de03b7e3448345a6c611fc96d9287",
          "message": "more test coverage",
          "timestamp": "2026-06-22T13:12:19+02:00",
          "tree_id": "4e7d907efc034ba9796d2580da02bc18c268bf07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/4c47254f985de03b7e3448345a6c611fc96d9287"
        },
        "date": 1782126880590,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "442bd30053d373021f390964218348a9512de5b2",
          "message": "Implemented the Redis transport end to end (missed files)",
          "timestamp": "2026-06-22T18:54:51+02:00",
          "tree_id": "cf38fd5a6b520278ac71920f6f329eb3ce9501f0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/442bd30053d373021f390964218348a9512de5b2"
        },
        "date": 1782147438942,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5a70579754ca2f51207cb9c6fd9f4681d0d529cf",
          "message": "Redis transport fixes",
          "timestamp": "2026-06-22T19:26:49+02:00",
          "tree_id": "0aff004e6924fb9cb656f2c8e583e4ae6b4a19d9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5a70579754ca2f51207cb9c6fd9f4681d0d529cf"
        },
        "date": 1782149820852,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "676e73cbdca8c6975ee7b9b6d61220e8659b4642",
          "message": "package bump",
          "timestamp": "2026-06-22T19:38:14+02:00",
          "tree_id": "651f1d6e0d6e1c83abe1535756905c38d937d452",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/676e73cbdca8c6975ee7b9b6d61220e8659b4642"
        },
        "date": 1782150500587,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a619da4e5271a0eb7adb52cc27fac70e8b3bf35d",
          "message": "Implemented NATS channel and transport",
          "timestamp": "2026-06-23T10:20:25+02:00",
          "tree_id": "7df45f29e08480bf8ccf543a91c7de47105bb6dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a619da4e5271a0eb7adb52cc27fac70e8b3bf35d"
        },
        "date": 1782203469379,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "78706e145b78cb241e547ddcdb73c8913a921b9b",
          "message": "Fixed NATS issues, added tests",
          "timestamp": "2026-06-23T11:24:19+02:00",
          "tree_id": "d0993e96aaccecda7fc145e3f8a483a1aadca718",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/78706e145b78cb241e547ddcdb73c8913a921b9b"
        },
        "date": 1782207570075,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "21a784c103f1c1c705195a58da20adc6425d007c",
          "message": "optimization",
          "timestamp": "2026-06-23T16:27:39+02:00",
          "tree_id": "7fbeb262c123e61793cda0685899f2f4bc34523e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/21a784c103f1c1c705195a58da20adc6425d007c"
        },
        "date": 1782225008755,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7be0409f6dd42811feab06ca36700a5651c6f75f",
          "message": "refactor: hardening and optimizations",
          "timestamp": "2026-06-24T16:06:48+02:00",
          "tree_id": "013ba14d18b3283a0e58dc77fc4bc1f420a710d5",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7be0409f6dd42811feab06ca36700a5651c6f75f"
        },
        "date": 1782311394458,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "67a76d645b0d5cd5117227fd338420064fcc2d6c",
          "message": "fixed loadtest apphost logging error",
          "timestamp": "2026-06-24T16:43:48+02:00",
          "tree_id": "0ffcee11ee0bd58b08b2ed5d84cf646530dd8bfe",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/67a76d645b0d5cd5117227fd338420064fcc2d6c"
        },
        "date": 1782312878881,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd",
          "message": "Made lost-subscriber recovery coherent for multiple recoverable waiters on one correlation id",
          "timestamp": "2026-06-29T10:36:44+02:00",
          "tree_id": "666953ca1f47ae96e2377b15a22bac3f0303adcd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd"
        },
        "date": 1782722354438,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5ff27f3ad385c13043a4bb6d7baabc612d9e9c53",
          "message": "Fixed exception stack traces are reset",
          "timestamp": "2026-06-29T10:52:54+02:00",
          "tree_id": "71de51782d1d8ea53d8f6f74204f34cc90f51af8",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5ff27f3ad385c13043a4bb6d7baabc612d9e9c53"
        },
        "date": 1782723803487,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7fa908b6b83b2dde7e515f43228a49722069fd49",
          "message": "In-Memory performance fix",
          "timestamp": "2026-06-29T11:23:34+02:00",
          "tree_id": "6915c4f995598ebc2c12837e4143aedddc85a38b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7fa908b6b83b2dde7e515f43228a49722069fd49"
        },
        "date": 1782725658626,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d",
          "message": "upgrade libs",
          "timestamp": "2026-06-29T11:35:35+02:00",
          "tree_id": "e0664c0bccd14d40756a5d087f5bc6ac66a281e6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d"
        },
        "date": 1782726384828,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "request_response_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "attach_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "worker_pubsub_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_success_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "multi_step_domain_failure_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "ambient_exception_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "shared_exception_fanout_redis throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "reply_target_pubsub throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_field throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_request_response_success throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_default_ack_observed throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_header throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_response_ingress_body throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_reply_target throughput",
            "value": 20,
            "unit": "req/s"
          },
          {
            "name": "nats_worker_ack_after_receive_observed throughput",
            "value": 20,
            "unit": "req/s"
          }
        ]
      }
    ],
    "AsyncResponse Load test - latency": [
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ffbb46cb8b4713afec451b9a02dc3de9585069dd",
          "message": "added NBomber load test",
          "timestamp": "2026-06-17T01:31:48+02:00",
          "tree_id": "ccb860d192b362baf5d11235c1ce3db34a0d2c39",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ffbb46cb8b4713afec451b9a02dc3de9585069dd"
        },
        "date": 1781652977664,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 12.54,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 14.42,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.01,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1005.06,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "153850349b435011c16eebed4de044bcc0ddaa7e",
          "message": "Refactored ClassifyOutcome in favor of ShouldResumeOnRecovery",
          "timestamp": "2026-06-17T16:57:01+02:00",
          "tree_id": "071b5f9af921a0da084c38210926e4a7e5b3f2b1",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/153850349b435011c16eebed4de044bcc0ddaa7e"
        },
        "date": 1781708559848,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1017.34,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1049.6,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 21.39,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 73.47,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1011.71,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1026.05,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "2f952d5b7b7f9263e434757c1b0da1d42fedca63",
          "message": "chore: cleanup/reanming",
          "timestamp": "2026-06-17T17:21:49+02:00",
          "tree_id": "2b738c83dddae1dd408f3e58fb597329bf65ea65",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/2f952d5b7b7f9263e434757c1b0da1d42fedca63"
        },
        "date": 1781710043678,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1013.25,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1029.63,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 20.3,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 39.2,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1008.64,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1020.42,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ebc07896c052d7f805ef223c02dfa22989cccdcd",
          "message": "Implemented Source-generated logging and extension",
          "timestamp": "2026-06-18T01:03:17+02:00",
          "tree_id": "c4d501288f636132f27750f885d5bea3642ddf68",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ebc07896c052d7f805ef223c02dfa22989cccdcd"
        },
        "date": 1781737670233,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1005.57,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 12.6,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 13.85,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1004.54,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c4f5bf04253150fa84ede2b59904e7c1c0f842d6",
          "message": "Optimized reflection invocation",
          "timestamp": "2026-06-18T01:43:17+02:00",
          "tree_id": "13edc6b568bd797ef2ecbbe5b0c2d47caf7159fc",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c4f5bf04253150fa84ede2b59904e7c1c0f842d6"
        },
        "date": 1781740068252,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 13.18,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 14.65,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1004.54,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1076187437db7250b4004ade902f9a29f3be0a3a",
          "message": "Implemented AsyncResponseDiagnostics",
          "timestamp": "2026-06-18T02:23:19+02:00",
          "tree_id": "74343527b6844a6ec95393f0eaf58e8f09e84d2e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1076187437db7250b4004ade902f9a29f3be0a3a"
        },
        "date": 1781742483474,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 12.82,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 14.35,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.01,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1004.54,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "bfc3f5edbb30e7bab9c88d15df588817189ae9f5",
          "message": "Tests coverage expansion",
          "timestamp": "2026-06-18T09:14:16+02:00",
          "tree_id": "a8647fcaa34acd2d751d8394d5fd03c12fc4fb77",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/bfc3f5edbb30e7bab9c88d15df588817189ae9f5"
        },
        "date": 1781767010626,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1009.66,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1022.46,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 17.3,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 34.34,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1018.88,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "d5b3851d0d74c8482c267762e70caf7f58f4f293",
          "message": "IAsyncResponsePublisher.SetResponse<T> is now constrained to where T : IAsyncResponsePayload",
          "timestamp": "2026-06-18T10:03:56+02:00",
          "tree_id": "50180d8491650ae9fa26b68bcc8b5adf95ebbeab",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/d5b3851d0d74c8482c267762e70caf7f58f4f293"
        },
        "date": 1781769988784,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_redis p95 latency",
            "value": 1008.13,
            "unit": "ms"
          },
          {
            "name": "request_response_redis p99 latency",
            "value": 1018.37,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p95 latency",
            "value": 16.38,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub p99 latency",
            "value": 24.88,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1016.83,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "972a7e1543b09e0ba9a5455b1560145ed80292c1",
          "message": "Implemented the benchmark/load-test expansion end to end",
          "timestamp": "2026-06-18T11:05:58+02:00",
          "tree_id": "485ea28888c96763ab19ee064ec892b43578e8cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/972a7e1543b09e0ba9a5455b1560145ed80292c1"
        },
        "date": 1781774047429,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1007.62,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1008.13,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 57.95,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 60.35,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.64,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.9,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.55,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 6.45,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.28,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 4.14,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e",
          "message": "fixed in-memory waiter timer could race cleanup",
          "timestamp": "2026-06-18T11:55:14+02:00",
          "tree_id": "b367b0e04256a6f5a8564598ae67835b375bc3d6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6dcb3a4bccf01a3f9a3777c6b7fe74dbacbb1f0e"
        },
        "date": 1781776992128,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1008.64,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1009.15,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.62,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 62.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2012.16,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.87,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 5.82,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.99,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 6.21,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.46,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 4.54,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b",
          "message": "Implemented the performance/allocation",
          "timestamp": "2026-06-18T14:35:14+02:00",
          "tree_id": "a1588a9b54a77bae3df547ec3acfa5e1dc0ea3a7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3ddf55cb41d8c44bbdb6633bf7c9962b9759fe0b"
        },
        "date": 1781786268097,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1017.34,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1013.76,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1007.62,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 52.67,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 68.8,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2021.38,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2017.28,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.36,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 5.98,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.21,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 7.67,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 3.12,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 6.9,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "15bd5d6f3d11509bb5892de59d762889abbd5404",
          "message": "fix stress test harness",
          "timestamp": "2026-06-18T15:00:25+02:00",
          "tree_id": "991639655faaef8013e8004d3ff5bacaaa0ed662",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/15bd5d6f3d11509bb5892de59d762889abbd5404"
        },
        "date": 1781787778630,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1008.64,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1010.18,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1007.62,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.68,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 62.56,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2014.21,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.8,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.3,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.81,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 6.45,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.96,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 6.92,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "677e678174e24eb024297dbe53c45145c2ecf137",
          "message": "Performance improvements",
          "timestamp": "2026-06-18T15:51:13+02:00",
          "tree_id": "af70395d47c9bcf88dc29a7d5f10ed4abe38030b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/677e678174e24eb024297dbe53c45145c2ecf137"
        },
        "date": 1781791151698,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 56.77,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 60.8,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2010.11,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2012.16,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.62,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 3.59,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.74,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.61,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.58,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 4.3,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "394284cca7942c20224fd400d3e489fbe22529d4",
          "message": "Restored the typed DispatchResponseCoreAsync",
          "timestamp": "2026-06-18T19:10:42+02:00",
          "tree_id": "6a2302c9e30b9081bd4153efd804c14d2ad8f0d0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/394284cca7942c20224fd400d3e489fbe22529d4"
        },
        "date": 1781802811318,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1005.57,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1010.69,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1005.57,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1011.71,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1012.74,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.04,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 63.01,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2016.26,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2013.18,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 3.22,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 5.47,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 4.05,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 6.27,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 3.3,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 9.63,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "1bc203254299568b68f861be578ab6ac00ba7f06",
          "message": "extensive test coverage",
          "timestamp": "2026-06-18T20:04:38+02:00",
          "tree_id": "689e907f6b8413b6cd79ed03ad31498cb02d2b7a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/1bc203254299568b68f861be578ab6ac00ba7f06"
        },
        "date": 1781806022355,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1005.57,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1010.18,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.87,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 63.26,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2015.23,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2012.16,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 3.7,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 7.14,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.91,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.72,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 3.26,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 5.13,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b2b7198ad04d615dc67c887a8833153b5f000da4",
          "message": "Optimized context propagator and InMemoryRecoveryStateStore",
          "timestamp": "2026-06-18T20:59:24+02:00",
          "tree_id": "5dfc241eabebd6663bd8ddf74ea99542b923a9cb",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b2b7198ad04d615dc67c887a8833153b5f000da4"
        },
        "date": 1781809315396,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1008.13,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 54.72,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 60.38,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 3.02,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.86,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.61,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.49,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.85,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 5.3,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "ba59b12ce2698fbd413df7e07a67c7581f23b3fd",
          "message": "Fixed SetResponseCore/SetRawResponseJsonCore code duplication",
          "timestamp": "2026-06-19T11:33:23+02:00",
          "tree_id": "a38526b7b76cd516a7f2fd80dd5f06f0b5380b1a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/ba59b12ce2698fbd413df7e07a67c7581f23b3fd"
        },
        "date": 1781861733050,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.01,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 49.44,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 56.22,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2010.11,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.3,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.22,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.42,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.91,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.56,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 4.58,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a",
          "message": "Fixed latency regression",
          "timestamp": "2026-06-19T12:12:07+02:00",
          "tree_id": "26379ae08cbe1cd71814d960a21bc6579c351a33",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6ccf5e1a4c527bcc47b5efa917e6804b5dd5a65a"
        },
        "date": 1781864420218,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 55.9,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 59.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.54,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.85,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.43,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.5,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.39,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 3.76,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a95672f4df5a479d9f48b963078061da6aa84509",
          "message": "SetResponse hot path optimization",
          "timestamp": "2026-06-19T12:57:06+02:00",
          "tree_id": "1f0bcae2122dbcfdead7271a7134b90f5ffed9dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a95672f4df5a479d9f48b963078061da6aa84509"
        },
        "date": 1781866755108,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.3,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 62.3,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.18,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 3.96,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.25,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.09,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.28,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 3.71,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "b5b9a73f55bc399e5ec9114ed56d830f24d4d855",
          "message": "InMemoryAsyncResponseChannel perf optimization",
          "timestamp": "2026-06-19T13:53:24+02:00",
          "tree_id": "430c87e41f483954b7a059e3043d430bf4efa341",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/b5b9a73f55bc399e5ec9114ed56d830f24d4d855"
        },
        "date": 1781870139222,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1007.62,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 57.34,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 59.78,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2009.09,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2010.11,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.3,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.22,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 7.98,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.15,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 4.89,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "fe36405f03e470bc58ff4f78f9f2ba20725d526d",
          "message": "Implemented the opt-in Google Pub/Sub early-ACK path",
          "timestamp": "2026-06-19T15:46:06+02:00",
          "tree_id": "b43d143ce70f99303c7376e6623a0ff6882088e9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/fe36405f03e470bc58ff4f78f9f2ba20725d526d"
        },
        "date": 1781877294858,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1011.2,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1010.18,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.52,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 57.41,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 61.6,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2014.21,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2012.16,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.75,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 4.18,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.88,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 5.56,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 2.81,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 4.97,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047",
          "message": "Implemented the fixes and regression coverage",
          "timestamp": "2026-06-19T16:58:25+02:00",
          "tree_id": "8f76b30848a0900974d4f94cc10a6d8d6cbcc076",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dd5dd3f3c78a5e73b7a0a7c070dc4c4233a77047"
        },
        "date": 1781881242445,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1009.66,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1009.15,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1007.1,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.3,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 62.24,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2013.18,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2008.06,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 3,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 7.07,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 3.81,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 7.84,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 3.1,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 5.54,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "6200531259c7c8066e4f520476472ce14710c076",
          "message": "Implement strict selection for transports, removed WithWorkerTransport",
          "timestamp": "2026-06-21T21:53:07+02:00",
          "tree_id": "c7cf5368e928b7d4cd20ddf2f339c3f7e18a4d48",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/6200531259c7c8066e4f520476472ce14710c076"
        },
        "date": 1782071744063,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1033.73,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1096.7,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1038.85,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1154.05,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1018.88,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1076.22,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 65.18,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 125.5,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2062.34,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2205.7,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2078.72,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2232.32,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 14.56,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 61.54,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 20.13,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 63.87,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 17.68,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 105.54,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "c49198ec3adbfa666c5279ae16ddaec36180fb44",
          "message": "Implemented RabbitMQ transport (missed files)",
          "timestamp": "2026-06-21T22:44:41+02:00",
          "tree_id": "2973c02c0d02631a4fd163030b99b7e9cb1d0a07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/c49198ec3adbfa666c5279ae16ddaec36180fb44"
        },
        "date": 1782074820660,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1005.57,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1004.03,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1003.01,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1004.54,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 58.3,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 61.7,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2006.02,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2007.04,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2016.26,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 2.1,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 3.62,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 2.75,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 4.19,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 1.99,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 3.72,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "686b85a23c07f19ba921f2a1e68c232abd8cb596",
          "message": "Added missed RabbitMQ transport tests",
          "timestamp": "2026-06-21T23:33:43+02:00",
          "tree_id": "e978f3421bf474f0c4b15d8bcb963ef385f57f0a",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/686b85a23c07f19ba921f2a1e68c232abd8cb596"
        },
        "date": 1782078315823,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1008.64,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1019.9,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1008.64,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1026.05,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1012.74,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 56.13,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 60.54,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2014.21,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2029.57,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2013.18,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2025.47,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 5.62,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 13.4,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 7.34,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 14.89,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 5.74,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 10.93,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 5.14,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 8.1,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 25.1,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 46.27,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 23.94,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 42.98,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 5.73,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 10.7,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 6,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 18.85,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "37bf44eb46beda36fbd0802457d5b3c60f41af02",
          "message": "Fixed RabbitMQ issues",
          "timestamp": "2026-06-22T00:04:25+02:00",
          "tree_id": "b9335f02d1174db66f8774b231c9e6b2280c2856",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/37bf44eb46beda36fbd0802457d5b3c60f41af02"
        },
        "date": 1782079611505,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1006.08,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1012.74,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1012.74,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1005.06,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1009.15,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 40.32,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 48.13,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2016.26,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2011.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2021.38,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 4.9,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 8.74,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 6.78,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 9.98,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 4.89,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 8.95,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 6.42,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 11.14,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 21.38,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 35.36,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 21.07,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 27.44,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 4.98,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 10.06,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 7.62,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 15.96,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "cafc92ff97dac0cce4e301ef5228322f2c979cef",
          "message": "Implemented the recoverable-builder split",
          "timestamp": "2026-06-22T12:25:07+02:00",
          "tree_id": "0274ba7fb878a5231da1a658de8558283e00dfb7",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/cafc92ff97dac0cce4e301ef5228322f2c979cef"
        },
        "date": 1782124457261,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1009.66,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1024.51,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1010.18,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1016.83,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1007.62,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1020.42,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 60.29,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 70.78,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2016.26,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2031.62,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2015.23,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2025.47,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 7.28,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 13.68,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 9.1,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 16.29,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 6.96,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 11.56,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 7.61,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 12.7,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 27.7,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 41.28,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 26.22,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 39.55,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 6.52,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 9.58,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 9.95,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 15.3,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "4c47254f985de03b7e3448345a6c611fc96d9287",
          "message": "more test coverage",
          "timestamp": "2026-06-22T13:12:19+02:00",
          "tree_id": "4e7d907efc034ba9796d2580da02bc18c268bf07",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/4c47254f985de03b7e3448345a6c611fc96d9287"
        },
        "date": 1782126882293,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1009.15,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1014.78,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1008.13,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1012.74,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1009.66,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 47.74,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 52.42,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2014.21,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2026.5,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2014.21,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2023.42,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 6.53,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 12.26,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 7.92,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 11.86,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 6.76,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 12.48,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 7.62,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 10.83,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 27.81,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 71.61,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 28.24,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 49.22,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 6.13,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 11.22,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 9.18,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 18.48,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "442bd30053d373021f390964218348a9512de5b2",
          "message": "Implemented the Redis transport end to end (missed files)",
          "timestamp": "2026-06-22T18:54:51+02:00",
          "tree_id": "cf38fd5a6b520278ac71920f6f329eb3ce9501f0",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/442bd30053d373021f390964218348a9512de5b2"
        },
        "date": 1782147440587,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1018.88,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1033.21,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1019.9,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1044.48,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1014.27,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1027.07,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 61.54,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 67.39,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2034.69,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2057.22,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2028.54,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2043.9,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 12.86,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 23.58,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 17.76,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 27.2,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 12.53,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 20.58,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 17.01,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 24.82,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 52.93,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 81.79,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 49.92,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 70.91,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 12.71,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 17.57,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 17.17,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 25.07,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 54.18,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 65.06,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 60.35,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 80.32,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 64.61,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 82.05,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 15.16,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 21.04,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 54.05,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 62.82,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5a70579754ca2f51207cb9c6fd9f4681d0d529cf",
          "message": "Redis transport fixes",
          "timestamp": "2026-06-22T19:26:49+02:00",
          "tree_id": "0aff004e6924fb9cb656f2c8e583e4ae6b4a19d9",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5a70579754ca2f51207cb9c6fd9f4681d0d529cf"
        },
        "date": 1782149823473,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1031.17,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1088.51,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1029.12,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1048.58,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1021.44,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1039.36,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 58.18,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 74.18,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2039.81,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2067.46,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2042.88,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2068.48,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 19.39,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 44.03,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 24.29,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 40.96,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 18.05,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 28.06,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 21.12,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 40.35,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 70.27,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 104.7,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 66.94,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 97.09,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 19.36,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 28.56,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 21.44,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 43.71,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 55.42,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 71.74,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 66.88,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 84.67,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 69.18,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 84.8,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 18.53,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 32.93,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 56.51,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 67.2,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "676e73cbdca8c6975ee7b9b6d61220e8659b4642",
          "message": "package bump",
          "timestamp": "2026-06-22T19:38:14+02:00",
          "tree_id": "651f1d6e0d6e1c83abe1535756905c38d937d452",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/676e73cbdca8c6975ee7b9b6d61220e8659b4642"
        },
        "date": 1782150503156,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1009.15,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1016.83,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1008.13,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1018.88,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1006.59,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1014.27,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 60.96,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 64.48,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2015.23,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2020.35,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2016.26,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2021.38,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 6.04,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 10.26,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 8.26,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 15.88,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 6.03,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 11.39,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 7.82,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 11.42,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 24.56,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 31.92,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 24.58,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 35.04,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 6,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 11.38,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 7.88,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 11.48,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 50.24,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 53.38,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 52.29,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 61.25,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 52.51,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 56.77,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 5.1,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 9.9,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 50.34,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 53.89,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "a619da4e5271a0eb7adb52cc27fac70e8b3bf35d",
          "message": "Implemented NATS channel and transport",
          "timestamp": "2026-06-23T10:20:25+02:00",
          "tree_id": "7df45f29e08480bf8ccf543a91c7de47105bb6dd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/a619da4e5271a0eb7adb52cc27fac70e8b3bf35d"
        },
        "date": 1782203471129,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1038.34,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1068.03,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1035.26,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1057.79,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1030.14,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1059.84,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 67.58,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 100.67,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2059.26,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2105.34,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2056.19,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2073.6,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 28.51,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 43.55,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 37.79,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 63.71,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 26.58,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 43.01,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 29.87,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 81.92,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 97.09,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 126.72,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 99.71,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 136.57,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 23.5,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 43.46,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 28.94,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 75.39,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 58.21,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 89.47,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 77.57,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 179.71,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 81.28,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 112,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 23.73,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 71.74,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 59.1,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 67.39,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1054.72,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1087.49,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 22.85,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 51.17,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 55.55,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 110.91,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 54.85,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 88.13,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 33.38,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 66.24,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 27.2,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 36.83,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "78706e145b78cb241e547ddcdb73c8913a921b9b",
          "message": "Fixed NATS issues, added tests",
          "timestamp": "2026-06-23T11:24:19+02:00",
          "tree_id": "d0993e96aaccecda7fc145e3f8a483a1aadca718",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/78706e145b78cb241e547ddcdb73c8913a921b9b"
        },
        "date": 1782207571805,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1050.62,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1082.37,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1061.89,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1105.92,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1042.43,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1082.37,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 75.26,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 101.38,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2087.94,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2125.82,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2091.01,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2125.82,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 40.32,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 76.03,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 49.12,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 77.69,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 36.42,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 63.39,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 46.78,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 104.19,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 105.79,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 298.5,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 114.69,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 311.81,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 34.82,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 54.85,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 41.7,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 109.5,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 70.53,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 101.7,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 96.26,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 166.66,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 96.26,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 170.75,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 32.19,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 67.97,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 60.83,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 88.77,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1077.25,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1128.45,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 30.86,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 77.95,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 68.29,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 129.92,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 73.47,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 103.62,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 46.62,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 86.59,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 36.61,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 67.46,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "21a784c103f1c1c705195a58da20adc6425d007c",
          "message": "optimization",
          "timestamp": "2026-06-23T16:27:39+02:00",
          "tree_id": "7fbeb262c123e61793cda0685899f2f4bc34523e",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/21a784c103f1c1c705195a58da20adc6425d007c"
        },
        "date": 1782225010420,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1053.7,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1095.68,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1055.74,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1093.63,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1039.87,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1062.91,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 75.07,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 105.34,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2074.62,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2125.82,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2083.84,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2140.16,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 33.57,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 86.27,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 44.8,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 93.95,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 33.31,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 61.38,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 38.62,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 89.34,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 122.18,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 226.3,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 127.17,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 196.22,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 32.56,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 51.9,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 36.58,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 119.81,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 68.8,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 86.85,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 93.31,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 135.68,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 99.58,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 153.47,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 31.15,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 74.18,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 66.37,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 85.7,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1071.1,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1116.16,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 35.74,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 86.46,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 70.59,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 134.91,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 71.17,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 143.23,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 43.74,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 92.99,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 32.9,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 62.02,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7be0409f6dd42811feab06ca36700a5651c6f75f",
          "message": "refactor: hardening and optimizations",
          "timestamp": "2026-06-24T16:06:48+02:00",
          "tree_id": "013ba14d18b3283a0e58dc77fc4bc1f420a710d5",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7be0409f6dd42811feab06ca36700a5651c6f75f"
        },
        "date": 1782311396313,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1043.46,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1080.32,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1049.6,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1087.49,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1029.12,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1058.82,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 64.51,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 103.94,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2064.38,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2099.2,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2061.31,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2115.58,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 29.55,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 72.13,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 34.27,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 75.71,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 28.91,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 70.02,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 29.98,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 58.82,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 91.97,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 128.57,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 95.81,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 124.54,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 24.4,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 38.24,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 35.2,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 65.09,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 63.49,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 84.93,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 86.72,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 148.1,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 85.18,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 138.37,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 28.27,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 48.64,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 63.46,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 81.34,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1043.97,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1079.3,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 27.84,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 80.06,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 53.92,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 88.19,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 54.72,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 77.5,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 38.3,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 65.12,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 25.79,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 55.46,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "67a76d645b0d5cd5117227fd338420064fcc2d6c",
          "message": "fixed loadtest apphost logging error",
          "timestamp": "2026-06-24T16:43:48+02:00",
          "tree_id": "0ffcee11ee0bd58b08b2ed5d84cf646530dd8bfe",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/67a76d645b0d5cd5117227fd338420064fcc2d6c"
        },
        "date": 1782312880848,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1061.89,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1107.97,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1065.98,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1097.73,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1046.02,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1076.22,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 71.74,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 109.38,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2093.05,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2140.16,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2101.25,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2134.02,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 45.57,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 101.12,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 58.21,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 109.31,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 43.94,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 80.64,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 36.06,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 62.46,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 130.37,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 181.38,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 143.62,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 173.57,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 37.02,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 82.18,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 39.9,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 71.81,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 72.64,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 133.76,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 96.32,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 262.65,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 119.49,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 203.01,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 35.2,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 66.94,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 76.8,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 124.74,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1076.22,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1106.94,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 39.87,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 88.06,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 102.46,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 169.09,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 102.08,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 168.7,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 57.25,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 101.63,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 40.96,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 98.11,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd",
          "message": "Made lost-subscriber recovery coherent for multiple recoverable waiters on one correlation id",
          "timestamp": "2026-06-29T10:36:44+02:00",
          "tree_id": "666953ca1f47ae96e2377b15a22bac3f0303adcd",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/dbcdbd6ecec060c73887ee7bf2a8681a78bab3bd"
        },
        "date": 1782722356155,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1053.7,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1084.42,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1051.65,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1089.54,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1032.7,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1058.82,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 69.31,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 100.16,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2086.91,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2119.68,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2086.91,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2119.68,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 32.64,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 82.37,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 51.07,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 93.76,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 37.22,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 75.39,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 33.5,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 67.26,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 86.66,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 119.62,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 97.79,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 122.69,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 32.9,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 57.31,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 39.9,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 114.75,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 62.72,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 88.77,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 86.4,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 144.13,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 95.1,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 142.21,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 28.38,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 51.74,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 65.31,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 95.23,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1065.98,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1093.63,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 29.76,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 57.06,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 67.84,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 122.69,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 66.3,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 111.3,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 47.07,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 80.77,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 26.88,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 49.98,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "5ff27f3ad385c13043a4bb6d7baabc612d9e9c53",
          "message": "Fixed exception stack traces are reset",
          "timestamp": "2026-06-29T10:52:54+02:00",
          "tree_id": "71de51782d1d8ea53d8f6f74204f34cc90f51af8",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/5ff27f3ad385c13043a4bb6d7baabc612d9e9c53"
        },
        "date": 1782723805265,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1043.46,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1095.68,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1048.06,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1134.59,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1030.66,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1095.68,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 73.22,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 126.46,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2075.65,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2142.21,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2074.62,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2181.12,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 34.88,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 97.92,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 54.24,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 129.66,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 30.75,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 69.89,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 36.32,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 82.43,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 100.22,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 141.82,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 95.1,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 143.49,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 31.06,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 48.58,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 32.64,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 111.81,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 61.73,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 73.02,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 86.14,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 135.81,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 85.76,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 123.71,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 31.39,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 46.88,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 62.85,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 92.42,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1092.61,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1257.47,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 29.63,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 118.53,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 71.23,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 131.84,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 73.66,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 175.49,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 43.68,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 125.5,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 28.67,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 134.01,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "7fa908b6b83b2dde7e515f43228a49722069fd49",
          "message": "In-Memory performance fix",
          "timestamp": "2026-06-29T11:23:34+02:00",
          "tree_id": "6915c4f995598ebc2c12837e4143aedddc85a38b",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/7fa908b6b83b2dde7e515f43228a49722069fd49"
        },
        "date": 1782725660508,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1017.86,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1035.26,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1016.32,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1025.02,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1013.76,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1030.14,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 59.36,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 63.46,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2026.5,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2054.14,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2028.54,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2058.24,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 13.83,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 24.45,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 19.71,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 33.6,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 12.06,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 20.77,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 134.66,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 272.64,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 38.98,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 54.91,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 36.45,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 53.34,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 12.3,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 22.96,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 111.3,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 185.47,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 52.26,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 61.02,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 58.37,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 71.81,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 59.3,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 71.42,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 12.74,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 29.17,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 52.61,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 59.78,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1170.43,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1342.46,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 39.55,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 159.49,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 104.96,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 188.93,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 73.92,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 195.71,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 65.92,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 156.54,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 32.49,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 183.3,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "committer": {
            "email": "tyunisov@gmail.com",
            "name": "Sky4CE",
            "username": "Sky4CE"
          },
          "distinct": true,
          "id": "3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d",
          "message": "upgrade libs",
          "timestamp": "2026-06-29T11:35:35+02:00",
          "tree_id": "e0664c0bccd14d40756a5d087f5bc6ac66a281e6",
          "url": "https://github.com/Sky4CE/AsyncResponse/commit/3a556cfcbdc8b7bf270a5bb32e3077f4bfece22d"
        },
        "date": 1782726386593,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "request_response_success_redis p95 latency",
            "value": 1042.94,
            "unit": "ms"
          },
          {
            "name": "request_response_success_redis p99 latency",
            "value": 1087.49,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p95 latency",
            "value": 1043.97,
            "unit": "ms"
          },
          {
            "name": "request_response_domain_failure_redis p99 latency",
            "value": 1082.37,
            "unit": "ms"
          },
          {
            "name": "attach_redis p95 latency",
            "value": 1033.21,
            "unit": "ms"
          },
          {
            "name": "attach_redis p99 latency",
            "value": 1083.39,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p95 latency",
            "value": 75.58,
            "unit": "ms"
          },
          {
            "name": "worker_pubsub_observed p99 latency",
            "value": 97.41,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p95 latency",
            "value": 2068.48,
            "unit": "ms"
          },
          {
            "name": "multi_step_success_redis p99 latency",
            "value": 2105.34,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p95 latency",
            "value": 2075.65,
            "unit": "ms"
          },
          {
            "name": "multi_step_domain_failure_redis p99 latency",
            "value": 2121.73,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p95 latency",
            "value": 34.53,
            "unit": "ms"
          },
          {
            "name": "ambient_exception_redis p99 latency",
            "value": 65.5,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p95 latency",
            "value": 60.54,
            "unit": "ms"
          },
          {
            "name": "shared_exception_fanout_redis p99 latency",
            "value": 81.54,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p95 latency",
            "value": 31.81,
            "unit": "ms"
          },
          {
            "name": "reply_target_pubsub p99 latency",
            "value": 89.22,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p95 latency",
            "value": 26.77,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_default_ack_observed p99 latency",
            "value": 95.17,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p95 latency",
            "value": 88.64,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_header p99 latency",
            "value": 147.07,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p95 latency",
            "value": 85.63,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_response_ingress_body p99 latency",
            "value": 140.29,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p95 latency",
            "value": 28.11,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_reply_target p99 latency",
            "value": 41.86,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p95 latency",
            "value": 40.77,
            "unit": "ms"
          },
          {
            "name": "rabbitmq_worker_ack_after_enqueue_observed p99 latency",
            "value": 121.79,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p95 latency",
            "value": 62.3,
            "unit": "ms"
          },
          {
            "name": "redis_worker_default_ack_observed p99 latency",
            "value": 112.13,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p95 latency",
            "value": 94.91,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_field p99 latency",
            "value": 140.29,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p95 latency",
            "value": 87.42,
            "unit": "ms"
          },
          {
            "name": "redis_response_ingress_body p99 latency",
            "value": 124.99,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p95 latency",
            "value": 33.57,
            "unit": "ms"
          },
          {
            "name": "redis_reply_target p99 latency",
            "value": 57.57,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p95 latency",
            "value": 64.29,
            "unit": "ms"
          },
          {
            "name": "redis_worker_ack_after_enqueue_observed p99 latency",
            "value": 119.04,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p95 latency",
            "value": 1045.5,
            "unit": "ms"
          },
          {
            "name": "nats_request_response_success p99 latency",
            "value": 1059.84,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p95 latency",
            "value": 27.18,
            "unit": "ms"
          },
          {
            "name": "nats_worker_default_ack_observed p99 latency",
            "value": 52.13,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p95 latency",
            "value": 50.62,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_header p99 latency",
            "value": 64.45,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p95 latency",
            "value": 52.26,
            "unit": "ms"
          },
          {
            "name": "nats_response_ingress_body p99 latency",
            "value": 82.88,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p95 latency",
            "value": 38.66,
            "unit": "ms"
          },
          {
            "name": "nats_reply_target p99 latency",
            "value": 63.17,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p95 latency",
            "value": 26.16,
            "unit": "ms"
          },
          {
            "name": "nats_worker_ack_after_receive_observed p99 latency",
            "value": 67.01,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}