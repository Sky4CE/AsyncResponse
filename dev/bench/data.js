window.BENCHMARK_DATA = {
  "lastUpdate": 1781776990766,
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
      }
    ]
  }
}